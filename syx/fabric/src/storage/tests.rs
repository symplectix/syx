use std::sync::Arc;

use bytes::{
    BufMut,
    Bytes,
};
use content_addressing::{
    Chunking,
    Codec,
    ContentFlags,
    Digest,
    Hasher,
};
use object_store::ObjectStore;
use object_store::local::LocalFileSystem;
use object_store::memory::InMemory;

use super::*;

fn in_memory() -> Arc<dyn ObjectStore> {
    Arc::new(InMemory::new())
}

/// A `LocalFileSystem` backend, paired with the `TempDir` it's rooted at.
/// Callers must keep the `TempDir` alive for as long as the backend is used.
fn local_fs() -> (testing::TempDir, Arc<dyn ObjectStore>) {
    let tmp = testing::tempdir();
    let backend = Arc::new(LocalFileSystem::new_with_prefix(tmp.path()).unwrap());
    (tmp, backend)
}

/// Everything a test needs to drive a `Cas` against: a fresh in-memory
/// `slatedb::Db`, packs written to `packs_backend`, and blobs staged in
/// a `Forgetter` rooted at a fresh local `TempDir`. No test overrides
/// `cas_prefix`/chunking/encoding, so `cas()` just uses their defaults.
struct Env {
    _forgetter_dir: testing::TempDir,
    db: slatedb::Db,
    blobs: Arc<dyn ObjectStore>,
    forgetter: Arc<Forgetter>,
    staged: Arc<KeyDir>,
    flushing: Flushing,
}

impl Env {
    async fn with_threshold(blobs_backend: Arc<dyn ObjectStore>, threshold: u64) -> Self {
        let db = slatedb::Db::builder("test", in_memory()).build().await.unwrap();
        let forgetter_dir = testing::tempdir();
        let (forgetter, mut replayed) =
            Forgetter::open(forgetter_dir.path(), u16::MAX).await.unwrap();
        assert!(replayed.next().is_none());
        let forgetter = Arc::new(forgetter);
        let staged = Arc::new(KeyDir::rebuild(replayed, Codec::new()).await);
        let flushing = Flushing::new(threshold, std::time::Duration::from_secs(3600));
        Self {
            _forgetter_dir: forgetter_dir,
            db,
            blobs: blobs_backend,
            forgetter,
            staged,
            flushing,
        }
    }

    async fn new(blobs_backend: Arc<dyn ObjectStore>) -> Self {
        Self::with_threshold(blobs_backend, DEFAULT_FLUSH_THRESHOLD).await
    }

    fn cas(&self) -> Cas<'_> {
        Cas::new(
            &self.db,
            &self.blobs,
            &self.forgetter,
            &self.staged,
            DEFAULT_CAS_PREFIX,
            &self.flushing,
            Chunking::new(),
            Codec::new(),
        )
    }
}

/// Some chunk digest referenced by `exclude`'s own manifest, other than
/// `exclude` itself, for tests that need an existing chunk key to target
/// for corruption without independently recomputing chunk digests.
async fn any_key_except(cas: Cas<'_>, exclude: Digest) -> Digest {
    let (_, manifest_bytes) = cas.load(&exclude).await.unwrap().expect("manifest present");
    let manifest = decode_chunks(&manifest_bytes).unwrap();
    manifest
        .iter()
        .map(|e| e.digest)
        .find(|d| *d != exclude)
        .expect("multi-chunk content should store more than just the manifest")
}

fn encode(flags: ContentFlags, raw: Vec<u8>) -> Vec<u8> {
    Codec::new().encode(flags, raw)
}

#[tokio::test]
async fn a_single_chunks_digest_is_the_content_digest_not_a_wrapped_one() {
    // This is what makes a small standalone blob dedup against the
    // same content appearing as one chunk inside a larger blob: both
    // are keyed by the exact same digest. Runs against both inner
    // object stores, since this is a property of `cas`'s own digest
    // scheme, not of whichever store happens to be holding the packs.
    async fn check(cas: Cas<'_>) {
        let content = testing::random_bytes(4096); // well under CHUNK_MIN_SIZE
        let content_digest = Hasher::new().part(&content).digest();
        let d = cas.put(&Bytes::from(content)).await.unwrap();
        assert_eq!(d, content_digest);
    }

    check(Env::new(in_memory()).await.cas()).await;
    let (_tmp, inner) = local_fs();
    check(Env::new(inner).await.cas()).await;
}

#[tokio::test]
async fn identical_chunks_across_different_blobs_are_stored_once() {
    // Long enough, and shared for long enough, that content-defined
    // chunking is guaranteed to produce at least one identical cut
    // chunk in both blobs before they diverge.
    let shared = testing::random_bytes(Chunking::MAX_SIZE * 2);
    let blob_a = {
        let mut blob_a = shared.clone();
        blob_a.extend_from_slice(b"-a-suffix");
        Bytes::from(blob_a)
    };
    let blob_b = {
        let mut blob_b = shared;
        blob_b.extend_from_slice(b"-b-suffix");
        Bytes::from(blob_b)
    };

    // How many keys after putting `blob` and flushing it into packs.
    async fn count_keys(cas: Cas<'_>, blob: Bytes) -> usize {
        cas.put(&blob).await.unwrap();
        cas.flush_pending().await.unwrap();
        cas.entry_count().await.unwrap()
    }

    async fn check(cas: Cas<'_>, blob_a: &Bytes, blob_b: &Bytes, baseline: usize) {
        cas.put(blob_a).await.unwrap();
        cas.flush_pending().await.unwrap();
        let count_before = cas.entry_count().await.unwrap();
        cas.put(blob_b).await.unwrap();
        cas.flush_pending().await.unwrap();
        let count_after = cas.entry_count().await.unwrap();

        let new_keys = count_after - count_before;
        assert!(
            new_keys < baseline,
            "storing blob_b needed {new_keys} new keys, expected fewer than the {baseline} \
            it needs alone, since blob_a already stored the chunks they share"
        );
    }

    let mem_keys = count_keys(Env::new(in_memory()).await.cas(), blob_b.clone()).await;
    let (_tmp, inner) = local_fs();
    let tmp_keys = count_keys(Env::new(inner).await.cas(), blob_b.clone()).await;
    // The baseline is a property of blob_b's content and cas's chunking,
    // not of which backend computed it.
    assert_eq!(mem_keys, tmp_keys);

    check(Env::new(in_memory()).await.cas(), &blob_a, &blob_b, mem_keys).await;
    let (_tmp, inner) = local_fs();
    check(Env::new(inner).await.cas(), &blob_a, &blob_b, tmp_keys).await;
}

#[tokio::test]
async fn flush_pending_moves_a_staged_entry_out_of_the_forgetter_and_into_a_pack() {
    let env = Env::with_threshold(in_memory(), 1024 * 1024).await;
    let cas = env.cas();

    let content = Bytes::from_static(b"0123456789");
    let d = cas.put(&content).await.unwrap();
    assert!(env.staged.contains(d));
    assert!(cas.get_entry(d).await.unwrap().is_none());

    cas.flush_pending().await.unwrap();
    assert!(!env.staged.contains(d));
    assert!(cas.get_entry(d).await.unwrap().is_some());

    assert_eq!(cas.get::<Bytes>(&d).await.unwrap(), Some(content));
}

#[tokio::test]
async fn get_returns_invalid_data_for_tampered_content() {
    async fn check(cas: Cas<'_>) {
        let d = cas.put(&Bytes::from_static(b"hello")).await.unwrap();
        cas.flush_pending().await.unwrap();

        // Overwrite the stored bytes with content that doesn't hash
        // back to `d`, simulating corruption.
        let tampered = encode(ContentFlags::empty(), b"not hello".to_vec());
        cas.put_blob(d, Bytes::from(tampered)).await.unwrap();
        cas.flush_pending().await.unwrap();

        let err = cas.get::<Bytes>(&d).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    check(Env::new(in_memory()).await.cas()).await;
    let (_tmp, inner) = local_fs();
    check(Env::new(inner).await.cas()).await;
}

#[tokio::test]
async fn get_returns_invalid_data_for_a_tampered_chunk() {
    // Needs a real (non-manifest) key to target, so this one stays
    // `in_memory`-only rather than being generalized over the backend.
    let env = Env::new(in_memory()).await;
    let cas = env.cas();
    let content = testing::random_bytes(Chunking::MAX_SIZE * 2);
    let d = cas.put(&Bytes::from(content)).await.unwrap();
    cas.flush_pending().await.unwrap();

    let chunk_key = any_key_except(cas, d).await;
    let tampered = encode(ContentFlags::empty(), b"tampered chunk content".to_vec());
    cas.put_blob(chunk_key, Bytes::from(tampered)).await.unwrap();
    cas.flush_pending().await.unwrap();

    let err = cas.get::<Bytes>(&d).await.unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::InvalidData);
}

#[tokio::test]
async fn read_into_returns_invalid_data_for_tampered_content() {
    async fn check(cas: Cas<'_>) {
        let d = cas.put(&Bytes::from_static(b"hello")).await.unwrap();
        cas.flush_pending().await.unwrap();

        let tampered = encode(ContentFlags::empty(), b"not hello".to_vec());
        cas.put_blob(d, Bytes::from(tampered)).await.unwrap();
        cas.flush_pending().await.unwrap();

        let mut out = Vec::new();
        let err = cas.read_into(&d, &mut out).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    check(Env::new(in_memory()).await.cas()).await;
    let (_tmp, inner) = local_fs();
    check(Env::new(inner).await.cas()).await;
}

#[tokio::test]
async fn read_into_returns_invalid_data_for_a_tampered_chunk() {
    // Needs a real (non-manifest) key to target, so this one stays
    // `in_memory`-only rather than being generalized over the backend.
    let env = Env::new(in_memory()).await;
    let cas = env.cas();
    let content = testing::random_bytes(Chunking::MAX_SIZE * 2);
    let d = cas.put(&Bytes::from(content)).await.unwrap();
    cas.flush_pending().await.unwrap();

    let chunk_key = any_key_except(cas, d).await;
    let tampered = encode(ContentFlags::empty(), b"tampered chunk content".to_vec());
    cas.put_blob(chunk_key, Bytes::from(tampered)).await.unwrap();
    cas.flush_pending().await.unwrap();

    let mut out = Vec::new();
    let err = cas.read_into(&d, &mut out).await.unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::InvalidData);
}

#[tokio::test]
async fn get_returns_invalid_data_for_a_tampered_manifest() {
    async fn check(cas: Cas<'_>) {
        let content = testing::random_bytes(Chunking::MAX_SIZE * 2);
        let d = cas.put(&Bytes::from(content)).await.unwrap();
        cas.flush_pending().await.unwrap();

        let tampered = encode(ContentFlags::CHUNKED, b"not a valid manifest body".to_vec());
        cas.put_blob(d, Bytes::from(tampered)).await.unwrap();
        cas.flush_pending().await.unwrap();

        let err = cas.get::<Bytes>(&d).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    check(Env::new(in_memory()).await.cas()).await;
    let (_tmp, inner) = local_fs();
    check(Env::new(inner).await.cas()).await;
}

#[tokio::test]
async fn get_returns_invalid_data_when_manifest_references_a_missing_chunk() {
    async fn check(cas: Cas<'_>) {
        let (present_digest, present_raw) =
            (Hasher::new().part(b"present").digest(), b"present".to_vec());
        cas.put_blob(
            present_digest,
            Bytes::from(encode(ContentFlags::empty(), present_raw.clone())),
        )
        .await
        .unwrap();
        let missing_digest = Hasher::new().part(b"never written").digest();

        let mut manifest = Vec::new();
        manifest.put_slice(present_digest.as_ref());
        manifest.put_u32(present_raw.len() as u32);
        manifest.put_slice(missing_digest.as_ref());
        manifest.put_u32(13);

        let blob_digest = {
            let mut h = Hasher::new();
            h.parts([present_digest.as_ref(), missing_digest.as_ref()]);
            h.digest()
        };
        cas.put_blob(blob_digest, Bytes::from(encode(ContentFlags::CHUNKED, manifest)))
            .await
            .unwrap();
        cas.flush_pending().await.unwrap();

        let err = cas.get::<Bytes>(&blob_digest).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    check(Env::new(in_memory()).await.cas()).await;
    let (_tmp, inner) = local_fs();
    check(Env::new(inner).await.cas()).await;
}
