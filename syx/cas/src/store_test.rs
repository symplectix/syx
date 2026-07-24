use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{
    Arc,
    Mutex,
};

use rand::RngExt as _;
use tokio::{
    fs,
    task,
};

use super::*;

/// An in-memory `Storage`.
#[derive(Clone, Default)]
struct MemStorage(Arc<Mutex<HashMap<Vec<u8>, Bytes>>>);

impl Storage for MemStorage {
    async fn contains_blob(&self, key: &[u8]) -> io::Result<bool> {
        Ok(self.0.lock().unwrap().contains_key(key))
    }

    async fn get_blob(&self, key: &[u8]) -> io::Result<Option<Bytes>> {
        Ok(self.0.lock().unwrap().get(key).cloned())
    }

    async fn put_blob(&self, key: &[u8], bytes: Bytes) -> io::Result<()> {
        self.0.lock().unwrap().insert(key.to_vec(), bytes);
        Ok(())
    }
}

impl MemStorage {
    /// Any stored key other than `exclude`, to target for corruption
    /// without needing to independently recompute chunk digests.
    fn any_key_except(&self, exclude: &[u8]) -> Vec<u8> {
        self.0
            .lock()
            .unwrap()
            .keys()
            .find(|k| k.as_slice() != exclude)
            .expect("multi-chunk content should store more than just the manifest")
            .clone()
    }
}

/// A filesystem-backed `Storage`.
/// Owns its own `TempDir` directly, so a test using it
/// doesn't need to separately keep one alive.
struct TmpStorage(testing::TempDir);

impl TmpStorage {
    fn new() -> Self {
        Self(testing::tempdir())
    }

    fn path(&self, key: &[u8]) -> PathBuf {
        use std::fmt::Write as _;

        let mut hex = String::with_capacity(key.len() * 2 + 1);
        write!(hex, "{:02x}", key[0]).unwrap();
        hex.push('/');
        for b in &key[1..] {
            write!(hex, "{b:02x}").unwrap();
        }
        self.0.path().join(hex)
    }
}

impl Storage for TmpStorage {
    async fn contains_blob(&self, key: &[u8]) -> io::Result<bool> {
        fs::try_exists(self.path(key)).await
    }

    async fn get_blob(&self, key: &[u8]) -> io::Result<Option<Bytes>> {
        match fs::read(self.path(key)).await {
            Ok(bytes) => Ok(Some(Bytes::from(bytes))),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e),
        }
    }

    async fn put_blob(&self, key: &[u8], bytes: Bytes) -> io::Result<()> {
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

impl CountEntries for MemStorage {
    fn count(&self) -> usize {
        self.0.lock().unwrap().len()
    }
}

impl CountEntries for TmpStorage {
    fn count(&self) -> usize {
        std::fs::read_dir(self.0.path())
            .into_iter()
            .flatten()
            .filter_map(Result::ok)
            .map(|shard| std::fs::read_dir(shard.path()).into_iter().flatten().count())
            .sum()
    }
}

fn incompressible_bytes(len: usize) -> Vec<u8> {
    let mut out = vec![0u8; len];
    rand::rng().fill(&mut out[..]);
    out
}

fn encode(flags: entry::Flags, raw: Vec<u8>) -> Vec<u8> {
    entry::Encoder::default().encode(flags, raw)
}

#[test]
fn worth_compressing_is_true_for_repetitive_content() {
    assert!(entry::Encoder::default().worth_compressing(&[b'a'; 4096]));
}

#[test]
fn worth_compressing_is_false_for_random_content() {
    assert!(!entry::Encoder::default().worth_compressing(&incompressible_bytes(4096)));
}

#[test]
fn worth_compressing_is_false_for_empty_content() {
    assert!(!entry::Encoder::default().worth_compressing(&[]));
}

#[test]
fn an_overridden_sniff_max_ratio_leaves_the_rest_at_their_defaults() {
    let sample = incompressible_bytes(4096);
    assert!(!entry::Encoder::default().worth_compressing(&sample));
    // >1.0: zstd's frame overhead makes `compressed_len` a little
    // *larger* than random data's own length, not just equal to it.
    assert!(entry::Encoder::default().sniff_max_ratio(2.0).worth_compressing(&sample));
}

#[test]
fn encode_entry_round_trips_through_decode_entry() {
    for raw in [b"a".repeat(4096), incompressible_bytes(4096)] {
        let stored = encode(entry::Flags::empty(), raw.clone());
        let (flags, decoded) = entry::Decoder.decode(Bytes::from(stored)).unwrap();
        assert!(!flags.contains(entry::Flags::MANIFEST));
        // `decoded` is always plain bytes regardless of whether it
        // was compressed on disk, so the returned flags shouldn't
        // claim it's still compressed.
        assert!(!flags.contains(entry::Flags::COMPRESSED));
        assert_eq!(decoded, raw);
    }
}

#[tokio::test]
async fn a_single_chunks_digest_is_the_content_digest_not_a_wrapped_one() {
    // This is what makes a small standalone blob dedup against the
    // same content appearing as one chunk inside a larger blob: both
    // are keyed by the exact same digest. Runs against both backends,
    // since this is a property of `cas`'s own digest scheme, not of
    // whichever `Storage` happens to be behind it.
    async fn check(storage: impl Storage) {
        let content = incompressible_bytes(4096); // well under CHUNK_MIN_SIZE
        let content_digest = digest_of(&content);
        let d = put(&storage, &Bytes::from(content)).await.unwrap();
        assert_eq!(d, content_digest);
    }

    check(MemStorage::default()).await;
    check(TmpStorage::new()).await;
}

#[tokio::test]
async fn identical_chunks_across_different_blobs_are_stored_once() {
    // Long enough, and shared for long enough, that content-defined
    // chunking is guaranteed to produce at least one identical cut
    // chunk in both blobs before they diverge.
    let shared = incompressible_bytes(consts::CHUNK_MAX_SIZE * 2);
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
    async fn count_keys(storage: impl Storage + CountEntries, blob: Bytes) -> usize {
        put(&storage, &blob).await.unwrap();
        storage.count()
    }

    async fn check(
        storage: impl Storage + CountEntries,
        blob_a: &Bytes,
        blob_b: &Bytes,
        baseline: usize,
    ) {
        put(&storage, blob_a).await.unwrap();
        let count_before = storage.count();
        put(&storage, blob_b).await.unwrap();
        let count_after = storage.count();

        let new_keys = count_after - count_before;
        assert!(
            new_keys < baseline,
            "storing blob_b needed {new_keys} new keys, expected fewer than the {baseline} \
            it needs alone, since blob_a already stored the chunks they share"
        );
    }

    let mem_keys = count_keys(MemStorage::default(), blob_b.clone()).await;
    let tmp_keys = count_keys(TmpStorage::new(), blob_b.clone()).await;
    // The baseline is a property of blob_b's content and cas's chunking,
    // not of which Storage backend computed it.
    assert_eq!(mem_keys, tmp_keys);

    check(MemStorage::default(), &blob_a, &blob_b, mem_keys).await;
    check(TmpStorage::new(), &blob_a, &blob_b, tmp_keys).await;
}

#[tokio::test]
async fn get_returns_invalid_data_for_tampered_content() {
    async fn check(storage: impl Storage) {
        let d = put(&storage, &Bytes::from_static(b"hello")).await.unwrap();

        // Overwrite the stored bytes with content that doesn't hash
        // back to `d`, simulating corruption.
        let tampered = encode(entry::Flags::empty(), b"not hello".to_vec());
        storage.put_blob(d.as_ref(), Bytes::from(tampered)).await.unwrap();

        let err = get::<_, Bytes>(&storage, &d).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    check(MemStorage::default()).await;
    check(TmpStorage::new()).await;
}

#[tokio::test]
async fn get_returns_invalid_data_for_a_tampered_chunk() {
    // Needs a real (non-manifest) key to target, so this one stays
    // `MemStorage`-only rather than being generalized over `Storage`.
    let storage = MemStorage::default();
    let content = incompressible_bytes(consts::CHUNK_MAX_SIZE * 2);
    let d = put(&storage, &Bytes::from(content)).await.unwrap();

    let chunk_key = storage.any_key_except(d.as_ref());
    let tampered = encode(entry::Flags::empty(), b"tampered chunk content".to_vec());
    storage.put_blob(&chunk_key, Bytes::from(tampered)).await.unwrap();

    let err = get::<_, Bytes>(&storage, &d).await.unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::InvalidData);
}

#[tokio::test]
async fn read_into_returns_invalid_data_for_tampered_content() {
    async fn check(storage: impl Storage) {
        let d = put(&storage, &Bytes::from_static(b"hello")).await.unwrap();

        let tampered = encode(entry::Flags::empty(), b"not hello".to_vec());
        storage.put_blob(d.as_ref(), Bytes::from(tampered)).await.unwrap();

        let mut out = Vec::new();
        let err = read_into(&storage, &d, &mut out).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    check(MemStorage::default()).await;
    check(TmpStorage::new()).await;
}

#[tokio::test]
async fn read_into_returns_invalid_data_for_a_tampered_chunk() {
    // Needs a real (non-manifest) key to target, so this one stays
    // `MemStorage`-only rather than being generalized over `Storage`.
    let storage = MemStorage::default();
    let content = incompressible_bytes(consts::CHUNK_MAX_SIZE * 2);
    let d = put(&storage, &Bytes::from(content)).await.unwrap();

    let chunk_key = storage.any_key_except(d.as_ref());
    let tampered = encode(entry::Flags::empty(), b"tampered chunk content".to_vec());
    storage.put_blob(&chunk_key, Bytes::from(tampered)).await.unwrap();

    let mut out = Vec::new();
    let err = read_into(&storage, &d, &mut out).await.unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::InvalidData);
}

#[tokio::test]
async fn get_returns_invalid_data_for_a_tampered_manifest() {
    async fn check(storage: impl Storage) {
        let content = incompressible_bytes(consts::CHUNK_MAX_SIZE * 2);
        let d = put(&storage, &Bytes::from(content)).await.unwrap();

        let tampered = encode(entry::Flags::MANIFEST, b"not a valid manifest body".to_vec());
        storage.put_blob(d.as_ref(), Bytes::from(tampered)).await.unwrap();

        let err = get::<_, Bytes>(&storage, &d).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    check(MemStorage::default()).await;
    check(TmpStorage::new()).await;
}

#[tokio::test]
async fn get_returns_invalid_data_when_manifest_references_a_missing_chunk() {
    async fn check(storage: impl Storage) {
        let (present_digest, present_raw) = (digest_of(b"present"), b"present".to_vec());
        storage
            .put_blob(
                present_digest.as_ref(),
                Bytes::from(encode(entry::Flags::empty(), present_raw.clone())),
            )
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
            .put_blob(blob_digest.as_ref(), Bytes::from(encode(entry::Flags::MANIFEST, manifest)))
            .await
            .unwrap();

        let err = get::<_, Bytes>(&storage, &blob_digest).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    check(MemStorage::default()).await;
    check(TmpStorage::new()).await;
}
