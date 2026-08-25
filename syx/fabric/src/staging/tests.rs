use std::path::Path;

use content_addressing::ContentFlags;
use tokio::io::AsyncWriteExt as _;

use super::*;

/// A `(key, value)` pair shaped like what `Cas::save` would stage: `value`
/// is `payload` run through `Codec::encode`, `key` is `payload`'s own
/// pre-encode digest.
fn encode(payload: &[u8]) -> (Digest, Bytes) {
    let key = Hasher::new().part(payload).digest();
    let value = Codec::new().encode(ContentFlags::empty(), payload.to_vec());
    (key, Bytes::from(value))
}

/// `Staging::open` with a `max_pending` high enough that no test other
/// than `put_refuses_once_max_pending_segments_are_stuck` needs to think
/// about it.
async fn open(dir: impl AsRef<Path>) -> Staging {
    Staging::open(dir.as_ref(), Codec::new(), u16::MAX).await.unwrap()
}

#[tokio::test]
async fn put_then_get_returns_the_same_bytes() {
    let dir = testing::tempdir();
    let staging = open(dir.path()).await;
    let (key, value) = encode(b"hello");

    staging.put(key, value.clone()).await.unwrap();

    assert_eq!(staging.get(key).await.unwrap(), Some(value));
}

#[tokio::test]
async fn contains_reflects_whether_a_key_is_staged() {
    let dir = testing::tempdir();
    let staging = open(dir.path()).await;
    let (key, value) = encode(b"hello");

    assert!(!staging.contains(key).await);
    staging.put(key, value).await.unwrap();
    assert!(staging.contains(key).await);
}

#[tokio::test]
async fn active_segment_len_tracks_bytes_written_to_the_active_segment() {
    let dir = testing::tempdir();
    let staging = open(dir.path()).await;
    let (key, value) = encode(b"hello");
    let expected = RECORD_HEADER_LEN + value.len() as u64;

    assert_eq!(staging.active_segment_len(), 0);
    staging.put(key, value).await.unwrap();
    assert_eq!(staging.active_segment_len(), expected);
}

#[tokio::test]
async fn rotate_moves_the_active_segment_into_pending_and_resets_active_segment_len() {
    let dir = testing::tempdir();
    let staging = open(dir.path()).await;
    let (key, value) = encode(b"hello");
    staging.put(key, value).await.unwrap();

    let segment = staging.rotate().await.unwrap();

    assert_eq!(staging.active_segment_len(), 0);
    assert_eq!(staging.pending_segments(), vec![segment]);
    assert!(staging.contains(key).await);
}

#[tokio::test]
async fn entries_returns_everything_written_to_a_pending_segment() {
    let dir = testing::tempdir();
    let staging = open(dir.path()).await;
    let (key_a, value_a) = encode(b"a");
    let (key_b, value_b) = encode(b"bb");
    staging.put(key_a, value_a.clone()).await.unwrap();
    staging.put(key_b, value_b.clone()).await.unwrap();

    let segment = staging.rotate().await.unwrap();

    // Unordered: `entries` no longer promises write order now that
    // `Pending::records` is keyed by digest, not a sequence.
    let entries = staging.entries(segment).await.unwrap();
    assert_eq!(entries.len(), 2);
    assert!(entries.contains(&(key_a, value_a)));
    assert!(entries.contains(&(key_b, value_b)));
}

#[tokio::test]
async fn finish_deletes_the_segment_and_evicts_its_entries() {
    let dir = testing::tempdir();
    let staging = open(dir.path()).await;
    let (key, value) = encode(b"hello");
    staging.put(key, value).await.unwrap();
    let segment = staging.rotate().await.unwrap();

    staging.finish(segment).await.unwrap();

    assert!(staging.pending_segments().is_empty());
    assert!(!staging.contains(key).await);
}

#[tokio::test]
async fn reopening_finds_pending_segments_left_by_a_previous_instance() {
    let dir = testing::tempdir();
    let (key, value) = encode(b"hello");
    let segment = {
        let staging = open(dir.path()).await;
        staging.put(key, value.clone()).await.unwrap();
        staging.rotate().await.unwrap()
    };

    let reopened = open(dir.path()).await;

    assert_eq!(reopened.pending_segments(), vec![segment]);
    assert_eq!(reopened.get(key).await.unwrap(), Some(value));
}

#[tokio::test]
async fn reopening_drops_a_torn_tail_record_and_keeps_the_valid_ones() {
    let dir = testing::tempdir();
    let (key, value) = encode(b"hello");
    {
        let staging = open(dir.path()).await;
        staging.put(key, value.clone()).await.unwrap();
    }

    // Simulate a crash mid-write: append a record whose declared length
    // promises more bytes than actually follow it in the file. Found by
    // listing `dir` (exactly one segment file exists at this point)
    // rather than assuming any particular id: `file_id`'s counter is
    // shared process-wide, so this directory's first segment isn't
    // necessarily id 0 if another test already claimed lower ids.
    let path = {
        let mut read_dir = fs::read_dir(dir.path()).await.unwrap();
        read_dir.next_entry().await.unwrap().unwrap().path()
    };
    let valid_len = fs::metadata(&path).await.unwrap().len();
    let mut torn = Vec::new();
    torn.extend_from_slice(Digest::new([0xaa; 32]).as_ref());
    torn.extend_from_slice(&100u32.to_be_bytes());
    torn.extend_from_slice(b"not enough bytes");
    // `sync_all` matters here: without it, the write isn't guaranteed
    // visible yet to the separate `fs::metadata` call below, since
    // `write_all` alone only guarantees the bytes were handed to the OS,
    // not that a concurrent reader through a different handle sees them.
    let mut f = fs::OpenOptions::new().append(true).open(&path).await.unwrap();
    f.write_all(&torn).await.unwrap();
    f.sync_all().await.unwrap();
    drop(f);
    assert!(fs::metadata(&path).await.unwrap().len() > valid_len);

    let reopened = open(dir.path()).await;

    assert_eq!(reopened.get(key).await.unwrap(), Some(value));
    // The file on disk was truncated back to just the valid record.
    assert_eq!(fs::metadata(&path).await.unwrap().len(), valid_len);

    // Appends after reopening land right after the truncated point, not
    // after the torn bytes.
    let (key2, value2) = encode(b"world");
    reopened.put(key2, value2.clone()).await.unwrap();
    assert_eq!(reopened.get(key2).await.unwrap(), Some(value2));
}

#[tokio::test]
async fn reopening_leaves_a_short_foreign_file_untouched() {
    let dir = testing::tempdir();

    // Matches `FileId`'s `{20-digit}.log` naming, but its few bytes
    // can't possibly hold `MAGIC`. Guessing this is one of `staging`'s
    // own crashed segments would mean deleting a file that was never
    // staging's to touch.
    let path = dir.path().join(format!("{:020}.log", 42u64));
    fs::write(&path, b"hi").await.unwrap();

    let staging = open(dir.path()).await;

    assert_eq!(fs::read(&path).await.unwrap(), b"hi");
    assert!(staging.pending_segments().is_empty());
}

#[tokio::test]
async fn reopening_leaves_an_empty_foreign_file_untouched() {
    let dir = testing::tempdir();

    // 0 bytes is not proof this is one of `staging`'s own.
    let path = dir.path().join(format!("{:020}.log", 43u64));
    fs::write(&path, b"").await.unwrap();

    let staging = open(dir.path()).await;

    assert!(fs::try_exists(&path).await.unwrap());
    assert!(staging.pending_segments().is_empty());
}

#[tokio::test]
async fn put_refuses_once_max_pending_segments_are_stuck() {
    let dir = testing::tempdir();
    let staging = Staging::open(dir.path(), Codec::new(), 2).await.unwrap();

    // Stage and rotate twice, reaching the cap without ever `finish`ing
    // a segment, as if `flush_pending` were failing persistently.
    for payload in [&b"a"[..], &b"b"[..]] {
        let (key, value) = encode(payload);
        staging.put(key, value).await.unwrap();
        staging.rotate().await.unwrap();
    }
    assert_eq!(staging.pending_segments().len(), 2);

    let (key, value) = encode(b"c");
    let err = staging.put(key, value.clone()).await.unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::Other);

    // Finishing one segment frees a slot for the next write.
    let oldest = staging.pending_segments()[0];
    staging.finish(oldest).await.unwrap();
    staging.put(key, value.clone()).await.unwrap();
    assert_eq!(staging.get(key).await.unwrap(), Some(value));
}
