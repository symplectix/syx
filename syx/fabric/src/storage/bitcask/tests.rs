use content_addressing::ContentFlags;

use super::*;

/// A `(key, value)` pair shaped like what `Cas::save` would stage: `value`
/// is `payload` run through `Codec::encode`, `key` is `payload`'s own
/// pre-encode digest.
fn encode(payload: &[u8]) -> (Digest, Bytes) {
    let key = Hasher::new().part(payload).digest();
    let value = Codec::new().encode(ContentFlags::empty(), payload.to_vec());
    (key, Bytes::from(value))
}

#[tokio::test]
async fn put_then_get_returns_the_same_bytes() {
    let dir = testing::tempdir();
    let bitcask = Bitcask::open(dir.path(), Codec::new()).await.unwrap();
    let (key, value) = encode(b"hello");

    bitcask.put(key, value.clone()).await.unwrap();

    assert_eq!(bitcask.get(key).await.unwrap(), Some(value));
}

#[tokio::test]
async fn contains_reflects_whether_a_key_is_staged() {
    let dir = testing::tempdir();
    let bitcask = Bitcask::open(dir.path(), Codec::new()).await.unwrap();
    let (key, value) = encode(b"hello");

    assert!(!bitcask.contains(key).await);
    bitcask.put(key, value).await.unwrap();
    assert!(bitcask.contains(key).await);
}

#[tokio::test]
async fn active_len_tracks_bytes_written_to_the_active_segment() {
    let dir = testing::tempdir();
    let bitcask = Bitcask::open(dir.path(), Codec::new()).await.unwrap();
    let (key, value) = encode(b"hello");
    let expected = RECORD_HEADER_LEN + value.len() as u64;

    assert_eq!(bitcask.active_len(), 0);
    bitcask.put(key, value).await.unwrap();
    assert_eq!(bitcask.active_len(), expected);
}

#[tokio::test]
async fn rotate_moves_the_active_segment_into_pending_and_resets_active_len() {
    let dir = testing::tempdir();
    let bitcask = Bitcask::open(dir.path(), Codec::new()).await.unwrap();
    let (key, value) = encode(b"hello");
    bitcask.put(key, value).await.unwrap();

    let segment = bitcask.rotate().await.unwrap();

    assert_eq!(bitcask.active_len(), 0);
    assert_eq!(bitcask.pending_segments().await, vec![segment]);
    assert!(bitcask.contains(key).await);
}

#[tokio::test]
async fn entries_returns_everything_written_to_a_pending_segment() {
    let dir = testing::tempdir();
    let bitcask = Bitcask::open(dir.path(), Codec::new()).await.unwrap();
    let (key_a, value_a) = encode(b"a");
    let (key_b, value_b) = encode(b"bb");
    bitcask.put(key_a, value_a.clone()).await.unwrap();
    bitcask.put(key_b, value_b.clone()).await.unwrap();

    let segment = bitcask.rotate().await.unwrap();

    let entries = bitcask.entries(segment).await.unwrap();
    assert_eq!(entries, vec![(key_a, value_a), (key_b, value_b)]);
}

#[tokio::test]
async fn finish_deletes_the_segment_and_evicts_its_entries() {
    let dir = testing::tempdir();
    let bitcask = Bitcask::open(dir.path(), Codec::new()).await.unwrap();
    let (key, value) = encode(b"hello");
    bitcask.put(key, value).await.unwrap();
    let segment = bitcask.rotate().await.unwrap();

    bitcask.finish(segment).await.unwrap();

    assert!(bitcask.pending_segments().await.is_empty());
    assert!(!bitcask.contains(key).await);
}

#[tokio::test]
async fn reopening_finds_pending_segments_left_by_a_previous_instance() {
    let dir = testing::tempdir();
    let (key, value) = encode(b"hello");
    let segment = {
        let bitcask = Bitcask::open(dir.path(), Codec::new()).await.unwrap();
        bitcask.put(key, value.clone()).await.unwrap();
        bitcask.rotate().await.unwrap()
    };

    let reopened = Bitcask::open(dir.path(), Codec::new()).await.unwrap();

    assert_eq!(reopened.pending_segments().await, vec![segment]);
    assert_eq!(reopened.get(key).await.unwrap(), Some(value));
}

#[tokio::test]
async fn reopening_drops_a_torn_tail_record_and_keeps_the_valid_ones() {
    let dir = testing::tempdir();
    let (key, value) = encode(b"hello");
    {
        let bitcask = Bitcask::open(dir.path(), Codec::new()).await.unwrap();
        bitcask.put(key, value.clone()).await.unwrap();
    }

    // Simulate a crash mid-write: append a record whose declared length
    // promises more bytes than actually follow it in the file.
    let path = segment_path(dir.path(), Segment::FIRST);
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

    let reopened = Bitcask::open(dir.path(), Codec::new()).await.unwrap();

    assert_eq!(reopened.get(key).await.unwrap(), Some(value));
    // The file on disk was truncated back to just the valid record.
    assert_eq!(fs::metadata(&path).await.unwrap().len(), valid_len);

    // Appends after reopening land right after the truncated point, not
    // after the torn bytes.
    let (key2, value2) = encode(b"world");
    reopened.put(key2, value2.clone()).await.unwrap();
    assert_eq!(reopened.get(key2).await.unwrap(), Some(value2));
}
