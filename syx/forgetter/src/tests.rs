use std::path::Path;

use tokio::io::AsyncWriteExt as _;

use super::*;
use crate::committer::RECORD_HEADER_LEN;

/// `Forgetter::open` with a `max_pending` high enough that no test other
/// than `save_refuses_once_max_pending_segments_are_stuck` needs to
/// think about it. Asserts replay found nothing, since every test opens
/// a fresh `TempDir`.
async fn open(dir: impl AsRef<Path>) -> Forgetter {
    let (forgetter, mut replayed) =
        Forgetter::open(dir.as_ref(), u16::MAX, u64::MAX, None).await.unwrap();
    assert!(replayed.next().is_none());
    forgetter
}

async fn read(locator: &Locator) -> Bytes {
    locator.bytes().await.unwrap()
}

#[tokio::test]
async fn save_then_find_returns_the_same_bytes() {
    let dir = testing::tempdir();
    let forgetter = open(dir.path()).await;
    let value = Bytes::from_static(b"hello");

    let locator = forgetter.save(value.clone()).await.unwrap();

    assert_eq!(read(&locator).await, value);
}

#[tokio::test]
async fn find_returns_none_for_an_unknown_file() {
    let dir = testing::tempdir();
    let forgetter = open(dir.path()).await;

    assert!(forgetter.find(file_id::next()).await.is_none());
}

#[tokio::test]
async fn active_segment_len_tracks_bytes_written_to_the_active_segment() {
    let dir = testing::tempdir();
    let forgetter = open(dir.path()).await;
    let value = Bytes::from_static(b"hello");
    let expected = RECORD_HEADER_LEN + value.len() as u64;

    assert_eq!(forgetter.active_segment_len(), 0);
    forgetter.save(value).await.unwrap();
    assert_eq!(forgetter.active_segment_len(), expected);
}

#[tokio::test]
async fn rotate_moves_the_active_segment_into_pending_and_resets_active_segment_len() {
    let dir = testing::tempdir();
    let forgetter = open(dir.path()).await;
    let value = Bytes::from_static(b"hello");
    let locator = forgetter.save(value.clone()).await.unwrap();

    let segment = forgetter.rotate().await.unwrap();

    assert_eq!(forgetter.active_segment_len(), 0);
    assert_eq!(forgetter.pending_segments(), vec![segment]);
    assert!(matches!(forgetter.find(segment).await, Some(Found::Sealed(_))));
    assert_eq!(read(&locator).await, value);
}

#[tokio::test]
async fn rotate_notifies_rotated() {
    let dir = testing::tempdir();
    let forgetter = open(dir.path()).await;
    let mut rotated = forgetter.rotated();
    forgetter.save(Bytes::from_static(b"hello")).await.unwrap();

    forgetter.rotate().await.unwrap();

    tokio::time::timeout(std::time::Duration::from_millis(200), rotated.changed())
        .await
        .expect("changed should resolve promptly")
        .expect("rotated should still be open for an explicit rotate");
}

#[tokio::test]
async fn dropping_forgetter_closes_rotated() {
    let dir = testing::tempdir();
    let forgetter = open(dir.path()).await;
    let mut rotated = forgetter.rotated();

    drop(forgetter);

    tokio::time::timeout(std::time::Duration::from_millis(200), rotated.changed())
        .await
        .expect("changed should resolve promptly")
        .expect_err("rotated should report closed once Forgetter is dropped");
}

#[tokio::test]
async fn save_rotates_the_active_segment_on_its_own_once_rotate_threshold_is_crossed() {
    let dir = testing::tempdir();
    let value = Bytes::from_static(b"hello");
    let threshold = RECORD_HEADER_LEN + value.len() as u64;
    let (forgetter, _) = Forgetter::open(dir.path(), u16::MAX, threshold, None).await.unwrap();
    let mut rotated = forgetter.rotated();

    let locator = forgetter.save(value.clone()).await.unwrap();

    assert_eq!(forgetter.active_segment_len(), 0);
    assert_eq!(forgetter.pending_segments(), vec![locator.file()]);
    assert!(matches!(forgetter.find(locator.file()).await, Some(Found::Sealed(_))));
    assert_eq!(read(&locator).await, value);
    tokio::time::timeout(std::time::Duration::from_millis(200), rotated.changed())
        .await
        .expect("changed should resolve promptly")
        .expect("rotated should still be open for a size-triggered rotation");
}

#[tokio::test]
async fn save_rotates_the_active_segment_on_its_own_once_rotate_after_elapses() {
    let dir = testing::tempdir();
    let value = Bytes::from_static(b"hello");
    let (forgetter, _) =
        Forgetter::open(dir.path(), u16::MAX, u64::MAX, Some(std::time::Duration::from_millis(20)))
            .await
            .unwrap();
    let locator = forgetter.save(value.clone()).await.unwrap();
    assert!(matches!(forgetter.find(locator.file()).await, Some(Found::Active(_))));

    // No mocked time available (tokio's `test-util` isn't enabled for
    // this crate), so this waits on the real clock; `rotate_after` is
    // kept short and this margin generous to avoid flakiness.
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    assert_eq!(forgetter.pending_segments(), vec![locator.file()]);
    assert!(matches!(forgetter.find(locator.file()).await, Some(Found::Sealed(_))));
    assert_eq!(read(&locator).await, value);
}

#[tokio::test]
async fn find_reports_active_vs_pending_correctly() {
    let dir = testing::tempdir();
    let forgetter = open(dir.path()).await;
    let locator = forgetter.save(Bytes::from_static(b"hello")).await.unwrap();

    assert!(matches!(forgetter.find(locator.file()).await, Some(Found::Active(_))));

    forgetter.rotate().await.unwrap();

    assert!(matches!(forgetter.find(locator.file()).await, Some(Found::Sealed(_))));
}

#[tokio::test]
async fn forget_deletes_the_segment() {
    let dir = testing::tempdir();
    let forgetter = open(dir.path()).await;
    forgetter.save(Bytes::from_static(b"hello")).await.unwrap();
    let segment = forgetter.rotate().await.unwrap();

    forgetter.forget(segment).await.unwrap();

    assert!(forgetter.pending_segments().is_empty());
    assert!(forgetter.find(segment).await.is_none());
}

#[tokio::test]
async fn reopening_finds_pending_segments_left_by_a_previous_instance() {
    let dir = testing::tempdir();
    let value = Bytes::from_static(b"hello");
    let (segment, locator) = {
        let forgetter = open(dir.path()).await;
        let locator = forgetter.save(value.clone()).await.unwrap();
        (forgetter.rotate().await.unwrap(), locator)
    };

    let (reopened, mut replayed) =
        Forgetter::open(dir.path(), u16::MAX, u64::MAX, None).await.unwrap();

    assert_eq!(reopened.pending_segments(), vec![segment]);
    let first = replayed.next().unwrap();
    assert!(replayed.next().is_none());
    assert_eq!(first.file(), locator.file());
    assert_eq!(first.slot(), locator.slot());
    assert_eq!(read(&first).await, value);
}

#[tokio::test]
async fn reopening_drops_a_torn_tail_record_and_keeps_the_valid_ones() {
    let dir = testing::tempdir();
    let value = Bytes::from_static(b"hello");
    {
        let forgetter = open(dir.path()).await;
        forgetter.save(value.clone()).await.unwrap();
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

    let (reopened, mut replayed) =
        Forgetter::open(dir.path(), u16::MAX, u64::MAX, None).await.unwrap();

    assert_eq!(read(&replayed.next().unwrap()).await, value);
    assert!(replayed.next().is_none());
    // The file on disk was truncated back to just the valid record.
    assert_eq!(fs::metadata(&path).await.unwrap().len(), valid_len);

    // Appends after reopening land right after the truncated point, not
    // after the torn bytes.
    let value2 = Bytes::from_static(b"world");
    let locator2 = reopened.save(value2.clone()).await.unwrap();
    assert_eq!(read(&locator2).await, value2);
}

#[tokio::test]
async fn reopening_leaves_a_short_foreign_file_untouched() {
    let dir = testing::tempdir();

    // Matches `FileId`'s `{20-digit}.log` naming, but its few bytes
    // can't possibly hold `MAGIC`. Guessing this is one of `forgetter`'s
    // own crashed segments would mean deleting a file that was never
    // forgetter's to touch.
    let path = dir.path().join(format!("{:020}.log", 42u64));
    fs::write(&path, b"hi").await.unwrap();

    let forgetter = open(dir.path()).await;

    assert_eq!(fs::read(&path).await.unwrap(), b"hi");
    assert!(forgetter.pending_segments().is_empty());
}

#[tokio::test]
async fn reopening_leaves_an_empty_foreign_file_untouched() {
    let dir = testing::tempdir();

    // 0 bytes is not proof this is one of `forgetter`'s own.
    let path = dir.path().join(format!("{:020}.log", 43u64));
    fs::write(&path, b"").await.unwrap();

    let forgetter = open(dir.path()).await;

    assert!(fs::try_exists(&path).await.unwrap());
    assert!(forgetter.pending_segments().is_empty());
}

#[tokio::test]
async fn save_refuses_once_max_pending_segments_are_stuck() {
    let dir = testing::tempdir();
    let (forgetter, _) = Forgetter::open(dir.path(), 2, u64::MAX, None).await.unwrap();

    // Stage and rotate twice, reaching the cap without ever forgetting a
    // segment, as if a caller-side flush were failing persistently.
    for payload in [&b"a"[..], &b"b"[..]] {
        forgetter.save(Bytes::copy_from_slice(payload)).await.unwrap();
        forgetter.rotate().await.unwrap();
    }
    assert_eq!(forgetter.pending_segments().len(), 2);

    let value = Bytes::from_static(b"c");
    let err = forgetter.save(value.clone()).await.unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::Other);

    // Forgetting one segment frees a slot for the next write.
    let oldest = forgetter.pending_segments()[0];
    forgetter.forget(oldest).await.unwrap();
    let locator = forgetter.save(value.clone()).await.unwrap();
    assert_eq!(read(&locator).await, value);
}
