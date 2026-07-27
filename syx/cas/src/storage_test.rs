use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{
    Arc,
    Mutex,
};

use bytes::{
    BufMut,
    Bytes,
};
use tokio::{
    fs,
    task,
};

use super::*;
use crate::hash::Hasher;

/// An in-memory `Reader`/`Writer`.
#[derive(Clone, Default)]
struct MemBackend(Arc<Mutex<HashMap<Digest, Bytes>>>);

impl Reader for MemBackend {
    async fn contains_blob(&self, key: Digest) -> io::Result<bool> {
        Ok(self.0.lock().unwrap().contains_key(&key))
    }

    async fn get_blob(&self, key: Digest) -> io::Result<Option<Bytes>> {
        Ok(self.0.lock().unwrap().get(&key).cloned())
    }
}

impl Writer for MemBackend {
    async fn put_blob(&self, key: Digest, bytes: Bytes) -> io::Result<()> {
        self.0.lock().unwrap().insert(key, bytes);
        Ok(())
    }
}

impl MemBackend {
    /// Any stored key other than `exclude`, to target for corruption
    /// without needing to independently recompute chunk digests.
    fn any_key_except(&self, exclude: Digest) -> Digest {
        *self
            .0
            .lock()
            .unwrap()
            .keys()
            .find(|k| **k != exclude)
            .expect("multi-chunk content should store more than just the manifest")
    }
}

/// A filesystem-backed `Reader`/`Writer`.
/// Owns its own `TempDir` directly, so a test using it
/// doesn't need to separately keep one alive.
struct TmpBackend(testing::TempDir);

impl TmpBackend {
    fn new() -> Self {
        Self(testing::tempdir())
    }

    fn path(&self, key: Digest) -> PathBuf {
        use std::fmt::Write as _;

        let key = key.as_ref();
        let mut hex = String::with_capacity(key.len() * 2 + 1);
        write!(hex, "{:02x}", key[0]).unwrap();
        hex.push('/');
        for b in &key[1..] {
            write!(hex, "{b:02x}").unwrap();
        }
        self.0.path().join(hex)
    }
}

impl Reader for TmpBackend {
    async fn contains_blob(&self, key: Digest) -> io::Result<bool> {
        fs::try_exists(self.path(key)).await
    }

    async fn get_blob(&self, key: Digest) -> io::Result<Option<Bytes>> {
        match fs::read(self.path(key)).await {
            Ok(bytes) => Ok(Some(Bytes::from(bytes))),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e),
        }
    }
}

impl Writer for TmpBackend {
    async fn put_blob(&self, key: Digest, bytes: Bytes) -> io::Result<()> {
        let path = self.path(key);
        // Sharding means the shard directory may not exist yet.
        let dir = path.parent().expect("path always has a parent").to_owned();
        fs::create_dir_all(&dir).await?;

        task::spawn_blocking(move || {
            use std::io::Write as _;

            let mut tmp = tempfile::NamedTempFile::new_in(&dir)?;
            tmp.write_all(&bytes)?;

            // A digest is only valid as long as the bytes it was
            // computed from never change, so make the file read-only
            // before it becomes visible under its final name.
            let mut perms = tmp.as_file().metadata()?.permissions();
            perms.set_readonly(true);
            tmp.as_file().set_permissions(perms)?;

            tmp.persist(&path).map_err(|e| e.error)?;
            Ok(())
        })
        .await
        .expect("blocking task should not panic")
    }
}

/// Inspect number of stored entries.
trait CountEntries {
    fn count(&self) -> usize;
}

impl CountEntries for MemBackend {
    fn count(&self) -> usize {
        self.0.lock().unwrap().len()
    }
}

impl CountEntries for TmpBackend {
    fn count(&self) -> usize {
        std::fs::read_dir(self.0.path())
            .into_iter()
            .flatten()
            .filter_map(Result::ok)
            .map(|shard| std::fs::read_dir(shard.path()).into_iter().flatten().count())
            .sum()
    }
}

fn encode(flags: Flags, raw: Vec<u8>) -> Vec<u8> {
    Encoding::new().encode(flags, raw)
}

#[test]
fn worth_compressing_is_true_for_repetitive_content() {
    assert!(Encoding::new().worth_compressing(&[b'a'; 4096]));
}

#[test]
fn worth_compressing_is_false_for_random_content() {
    assert!(!Encoding::new().worth_compressing(&testing::random_bytes(4096)));
}

#[test]
fn worth_compressing_is_false_for_empty_content() {
    assert!(!Encoding::new().worth_compressing(&[]));
}

#[test]
fn an_overridden_sniff_max_ratio_leaves_the_rest_at_their_defaults() {
    let sample = testing::random_bytes(4096);
    assert!(!Encoding::new().worth_compressing(&sample));
    // >1.0: zstd's frame overhead makes `compressed_len` a little
    // *larger* than random data's own length, not just equal to it.
    assert!(Encoding::new().sniff_max_ratio(2.0).worth_compressing(&sample));
}

#[test]
fn encode_entry_round_trips_through_decode_entry() {
    for raw in [b"a".repeat(4096), testing::random_bytes(4096)] {
        let stored = encode(Flags::empty(), raw.clone());
        let (flags, decoded) = Decoding::new().decode(Bytes::from(stored)).unwrap();
        assert!(!flags.contains(Flags::MANIFEST));
        // `decoded` is always plain bytes regardless of whether it
        // was compressed on disk, so the returned flags shouldn't
        // claim it's still compressed.
        assert!(!flags.contains(Flags::COMPRESSED));
        assert_eq!(decoded, raw);
    }
}

#[tokio::test]
async fn a_single_chunks_digest_is_the_content_digest_not_a_wrapped_one() {
    // This is what makes a small standalone blob dedup against the
    // same content appearing as one chunk inside a larger blob: both
    // are keyed by the exact same digest. Runs against both backends,
    // since this is a property of `cas`'s own digest scheme, not of
    // whichever backend happens to be behind it.
    async fn check(storage: impl Reader + Writer) {
        let content = testing::random_bytes(4096); // well under CHUNK_MIN_SIZE
        let content_digest = digest_of(&content);
        let d = Storage::new(&storage).put(&Bytes::from(content)).await.unwrap();
        assert_eq!(d, content_digest);
    }

    check(MemBackend::default()).await;
    check(TmpBackend::new()).await;
}

#[tokio::test]
async fn identical_chunks_across_different_blobs_are_stored_once() {
    // Long enough, and shared for long enough, that content-defined
    // chunking is guaranteed to produce at least one identical cut
    // chunk in both blobs before they diverge.
    let shared = testing::random_bytes(defaults::CHUNK_MAX_SIZE * 2);
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
    async fn count_keys(storage: impl Reader + Writer + CountEntries, blob: Bytes) -> usize {
        Storage::new(&storage).put(&blob).await.unwrap();
        storage.count()
    }

    async fn check(
        storage: impl Reader + Writer + CountEntries,
        blob_a: &Bytes,
        blob_b: &Bytes,
        baseline: usize,
    ) {
        let cas = Storage::new(&storage);
        cas.put(blob_a).await.unwrap();
        let count_before = storage.count();
        cas.put(blob_b).await.unwrap();
        let count_after = storage.count();

        let new_keys = count_after - count_before;
        assert!(
            new_keys < baseline,
            "storing blob_b needed {new_keys} new keys, expected fewer than the {baseline} \
            it needs alone, since blob_a already stored the chunks they share"
        );
    }

    let mem_keys = count_keys(MemBackend::default(), blob_b.clone()).await;
    let tmp_keys = count_keys(TmpBackend::new(), blob_b.clone()).await;
    // The baseline is a property of blob_b's content and cas's chunking,
    // not of which backend computed it.
    assert_eq!(mem_keys, tmp_keys);

    check(MemBackend::default(), &blob_a, &blob_b, mem_keys).await;
    check(TmpBackend::new(), &blob_a, &blob_b, tmp_keys).await;
}

#[tokio::test]
async fn get_returns_invalid_data_for_tampered_content() {
    async fn check(storage: impl Reader + Writer) {
        let cas = Storage::new(&storage);
        let d = cas.put(&Bytes::from_static(b"hello")).await.unwrap();

        // Overwrite the stored bytes with content that doesn't hash
        // back to `d`, simulating corruption.
        let tampered = encode(Flags::empty(), b"not hello".to_vec());
        storage.put_blob(d, Bytes::from(tampered)).await.unwrap();

        let err = cas.get::<Bytes>(&d).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    check(MemBackend::default()).await;
    check(TmpBackend::new()).await;
}

#[tokio::test]
async fn get_returns_invalid_data_for_a_tampered_chunk() {
    // Needs a real (non-manifest) key to target, so this one stays
    // `MemBackend`-only rather than being generalized over the backend.
    let storage = MemBackend::default();
    let content = testing::random_bytes(defaults::CHUNK_MAX_SIZE * 2);
    let cas = Storage::new(&storage);
    let d = cas.put(&Bytes::from(content)).await.unwrap();

    let chunk_key = storage.any_key_except(d);
    let tampered = encode(Flags::empty(), b"tampered chunk content".to_vec());
    storage.put_blob(chunk_key, Bytes::from(tampered)).await.unwrap();

    let err = cas.get::<Bytes>(&d).await.unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::InvalidData);
}

#[tokio::test]
async fn read_into_returns_invalid_data_for_tampered_content() {
    async fn check(storage: impl Reader + Writer) {
        let cas = Storage::new(&storage);
        let d = cas.put(&Bytes::from_static(b"hello")).await.unwrap();

        let tampered = encode(Flags::empty(), b"not hello".to_vec());
        storage.put_blob(d, Bytes::from(tampered)).await.unwrap();

        let mut out = Vec::new();
        let err = cas.read_into(&d, &mut out).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    check(MemBackend::default()).await;
    check(TmpBackend::new()).await;
}

#[tokio::test]
async fn read_into_returns_invalid_data_for_a_tampered_chunk() {
    // Needs a real (non-manifest) key to target, so this one stays
    // `MemBackend`-only rather than being generalized over the backend.
    let storage = MemBackend::default();
    let content = testing::random_bytes(defaults::CHUNK_MAX_SIZE * 2);
    let cas = Storage::new(&storage);
    let d = cas.put(&Bytes::from(content)).await.unwrap();

    let chunk_key = storage.any_key_except(d);
    let tampered = encode(Flags::empty(), b"tampered chunk content".to_vec());
    storage.put_blob(chunk_key, Bytes::from(tampered)).await.unwrap();

    let mut out = Vec::new();
    let err = cas.read_into(&d, &mut out).await.unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::InvalidData);
}

#[tokio::test]
async fn get_returns_invalid_data_for_a_tampered_manifest() {
    async fn check(storage: impl Reader + Writer) {
        let content = testing::random_bytes(defaults::CHUNK_MAX_SIZE * 2);
        let cas = Storage::new(&storage);
        let d = cas.put(&Bytes::from(content)).await.unwrap();

        let tampered = encode(Flags::MANIFEST, b"not a valid manifest body".to_vec());
        storage.put_blob(d, Bytes::from(tampered)).await.unwrap();

        let err = cas.get::<Bytes>(&d).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    check(MemBackend::default()).await;
    check(TmpBackend::new()).await;
}

#[tokio::test]
async fn get_returns_invalid_data_when_manifest_references_a_missing_chunk() {
    async fn check(storage: impl Reader + Writer) {
        let (present_digest, present_raw) = (digest_of(b"present"), b"present".to_vec());
        storage
            .put_blob(present_digest, Bytes::from(encode(Flags::empty(), present_raw.clone())))
            .await
            .unwrap();
        let missing_digest = digest_of(b"never written");

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
        storage
            .put_blob(blob_digest, Bytes::from(encode(Flags::MANIFEST, manifest)))
            .await
            .unwrap();

        let err = Storage::new(&storage).get::<Bytes>(&blob_digest).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    check(MemBackend::default()).await;
    check(TmpBackend::new()).await;
}
