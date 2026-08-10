//! Round-trips content through `cas::Storage`, including a blob large
//! enough to span multiple chunks (and so multiple staged entries plus
//! a manifest), and confirms staged entries do consolidate into pack
//! objects.

use std::sync::Arc;

use futures::StreamExt as _;
use object_store::memory::InMemory;

async fn packing_cas(packs_threshold: u64) -> (cas::Storage, Arc<dyn object_store::ObjectStore>) {
    let db_backend: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let inner: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let cas = cas::Storage::builder("test", db_backend)
        .packs(inner.clone())
        .packs_threshold(packs_threshold)
        .build()
        .await
        .unwrap();
    (cas, inner)
}

#[tokio::test]
async fn get_returns_what_was_put_for_a_small_blob() {
    let (cas, _inner) = packing_cas(1024 * 1024).await;

    let content = cas::Bytes::from_static(b"hello via a packing storage backend");
    let d = cas.put(&content).await.unwrap();

    assert_eq!(cas.get::<cas::Bytes>(&d).await.unwrap(), Some(content));
}

#[tokio::test]
async fn get_returns_what_was_put_for_a_multi_chunk_blob() {
    let (cas, _inner) = packing_cas(1024 * 1024).await;

    let content = cas::Bytes::from(testing::random_bytes(2 * 1024 * 1024));
    let d = cas.put(&content).await.unwrap();

    assert_eq!(cas.get::<cas::Bytes>(&d).await.unwrap(), Some(content));
}

#[tokio::test]
async fn a_full_pack_flushes_on_the_next_write_and_stays_readable() {
    // Small enough that a single chunk's write crosses the threshold.
    let (cas, inner) = packing_cas(8).await;

    let first = cas::Bytes::from_static(b"0123456789");
    let d1 = cas.put(&first).await.unwrap();
    // `put_blob` checks the threshold, and flushes if crossed, *before*
    // staging its own bytes -- so a flush failure never makes an
    // otherwise-successful write look like it failed. That means the
    // write that crosses the threshold isn't the one that gets packed;
    // the next one is.
    assert_eq!(inner.list(None).count().await, 0);

    let second = cas::Bytes::from_static(b"more");
    let d2 = cas.put(&second).await.unwrap();
    assert_eq!(inner.list(None).count().await, 1);

    assert_eq!(cas.get::<cas::Bytes>(&d1).await.unwrap(), Some(first));
    assert_eq!(cas.get::<cas::Bytes>(&d2).await.unwrap(), Some(second));
}
