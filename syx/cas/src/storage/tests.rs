use std::sync::Arc;

use bytes::{
    BufMut,
    Bytes,
};
use object_store::ObjectStore;
use object_store::local::LocalFileSystem;
use object_store::memory::InMemory;

use super::*;
use crate::hash::Hasher;

fn in_memory() -> Arc<dyn ObjectStore> {
    Arc::new(InMemory::new())
}

/// A `LocalFileSystem` backend, paired with the `TempDir` it's rooted at --
/// callers must keep the `TempDir` alive for as long as the backend is used.
fn local_fs() -> (testing::TempDir, Arc<dyn ObjectStore>) {
    let tmp = testing::tempdir();
    let backend = Arc::new(LocalFileSystem::new_with_prefix(tmp.path()).unwrap());
    (tmp, backend)
}

/// A `Storage` writing packs to `packs`, staged in its own in-memory
/// `slatedb` (staging durability isn't what these tests are about;
/// `packs` is the backend variety under test).
async fn packing(packs: Arc<dyn ObjectStore>) -> Storage {
    Storage::builder("test", in_memory()).packs(packs).build().await.unwrap()
}

/// Some chunk digest referenced by `exclude`'s own manifest, other than
/// `exclude` itself -- for tests that need an existing chunk key to
/// target for corruption without independently recomputing chunk digests.
async fn any_key_except(cas: &Storage, exclude: Digest) -> Digest {
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
    async fn check(cas: Storage) {
        let content = testing::random_bytes(4096); // well under CHUNK_MIN_SIZE
        let content_digest = Hasher::new().part(&content).digest();
        let d = cas.put(&Bytes::from(content)).await.unwrap();
        assert_eq!(d, content_digest);
    }

    check(packing(in_memory()).await).await;
    let (_tmp, inner) = local_fs();
    check(packing(inner).await).await;
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

    // How many keys after putting `blob`.
    async fn count_keys(cas: Storage, blob: Bytes) -> usize {
        cas.put(&blob).await.unwrap();
        cas.entry_count().await.unwrap()
    }

    async fn check(cas: Storage, blob_a: &Bytes, blob_b: &Bytes, baseline: usize) {
        cas.put(blob_a).await.unwrap();
        let count_before = cas.entry_count().await.unwrap();
        cas.put(blob_b).await.unwrap();
        let count_after = cas.entry_count().await.unwrap();

        let new_keys = count_after - count_before;
        assert!(
            new_keys < baseline,
            "storing blob_b needed {new_keys} new keys, expected fewer than the {baseline} \
            it needs alone, since blob_a already stored the chunks they share"
        );
    }

    let mem_keys = count_keys(packing(in_memory()).await, blob_b.clone()).await;
    let (_tmp, inner) = local_fs();
    let tmp_keys = count_keys(packing(inner).await, blob_b.clone()).await;
    // The baseline is a property of blob_b's content and cas's chunking,
    // not of which backend computed it.
    assert_eq!(mem_keys, tmp_keys);

    check(packing(in_memory()).await, &blob_a, &blob_b, mem_keys).await;
    let (_tmp, inner) = local_fs();
    check(packing(inner).await, &blob_a, &blob_b, tmp_keys).await;
}

#[tokio::test]
async fn flush_pending_flips_staged_entries_from_inline_to_packed() {
    let cas = Storage::builder("test", in_memory())
        .packs(in_memory())
        .packs_threshold(8)
        .build()
        .await
        .unwrap();

    let content = Bytes::from_static(b"0123456789");
    let d = cas.put(&content).await.unwrap();
    assert!(matches!(cas.stage.get(d).await.unwrap(), Some(Entry::Inline(_))));
    assert!(cas.stage.pending_bytes().await.unwrap() > 0);

    cas.flush_pending().await.unwrap();
    assert!(matches!(cas.stage.get(d).await.unwrap(), Some(Entry::Packed { .. })));
    assert_eq!(cas.stage.pending_bytes().await.unwrap(), 0);

    assert_eq!(cas.get::<Bytes>(&d).await.unwrap(), Some(content));
}

#[tokio::test]
async fn get_returns_invalid_data_for_tampered_content() {
    async fn check(cas: Storage) {
        let d = cas.put(&Bytes::from_static(b"hello")).await.unwrap();

        // Overwrite the stored bytes with content that doesn't hash
        // back to `d`, simulating corruption.
        let tampered = encode(ContentFlags::empty(), b"not hello".to_vec());
        cas.put_blob(d, Bytes::from(tampered)).await.unwrap();

        let err = cas.get::<Bytes>(&d).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    check(packing(in_memory()).await).await;
    let (_tmp, inner) = local_fs();
    check(packing(inner).await).await;
}

#[tokio::test]
async fn get_returns_invalid_data_for_a_tampered_chunk() {
    // Needs a real (non-manifest) key to target, so this one stays
    // `in_memory`-only rather than being generalized over the backend.
    let cas = packing(in_memory()).await;
    let content = testing::random_bytes(Chunking::MAX_SIZE * 2);
    let d = cas.put(&Bytes::from(content)).await.unwrap();

    let chunk_key = any_key_except(&cas, d).await;
    let tampered = encode(ContentFlags::empty(), b"tampered chunk content".to_vec());
    cas.put_blob(chunk_key, Bytes::from(tampered)).await.unwrap();

    let err = cas.get::<Bytes>(&d).await.unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::InvalidData);
}

#[tokio::test]
async fn read_into_returns_invalid_data_for_tampered_content() {
    async fn check(cas: Storage) {
        let d = cas.put(&Bytes::from_static(b"hello")).await.unwrap();

        let tampered = encode(ContentFlags::empty(), b"not hello".to_vec());
        cas.put_blob(d, Bytes::from(tampered)).await.unwrap();

        let mut out = Vec::new();
        let err = cas.read_into(&d, &mut out).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    check(packing(in_memory()).await).await;
    let (_tmp, inner) = local_fs();
    check(packing(inner).await).await;
}

#[tokio::test]
async fn read_into_returns_invalid_data_for_a_tampered_chunk() {
    // Needs a real (non-manifest) key to target, so this one stays
    // `in_memory`-only rather than being generalized over the backend.
    let cas = packing(in_memory()).await;
    let content = testing::random_bytes(Chunking::MAX_SIZE * 2);
    let d = cas.put(&Bytes::from(content)).await.unwrap();

    let chunk_key = any_key_except(&cas, d).await;
    let tampered = encode(ContentFlags::empty(), b"tampered chunk content".to_vec());
    cas.put_blob(chunk_key, Bytes::from(tampered)).await.unwrap();

    let mut out = Vec::new();
    let err = cas.read_into(&d, &mut out).await.unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::InvalidData);
}

#[tokio::test]
async fn get_returns_invalid_data_for_a_tampered_manifest() {
    async fn check(cas: Storage) {
        let content = testing::random_bytes(Chunking::MAX_SIZE * 2);
        let d = cas.put(&Bytes::from(content)).await.unwrap();

        let tampered = encode(ContentFlags::CHUNKED, b"not a valid manifest body".to_vec());
        cas.put_blob(d, Bytes::from(tampered)).await.unwrap();

        let err = cas.get::<Bytes>(&d).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    check(packing(in_memory()).await).await;
    let (_tmp, inner) = local_fs();
    check(packing(inner).await).await;
}

#[tokio::test]
async fn get_returns_invalid_data_when_manifest_references_a_missing_chunk() {
    async fn check(cas: Storage) {
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

        let err = cas.get::<Bytes>(&blob_digest).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    check(packing(in_memory()).await).await;
    let (_tmp, inner) = local_fs();
    check(packing(inner).await).await;
}
