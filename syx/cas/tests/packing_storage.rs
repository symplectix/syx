//! Round-trips content through `cas::Storage`, including a blob large
//! enough to span multiple chunks (and so multiple staged entries plus
//! a manifest), and confirms staged entries do consolidate into pack
//! objects.

use std::sync::Arc;

use futures::StreamExt as _;
use object_store::memory::InMemory;
use slatedb::Db;

async fn packing_cas(target_pack_bytes: u64) -> (cas::Storage, Arc<dyn object_store::ObjectStore>) {
    let db = Db::builder("test", Arc::new(InMemory::new()) as Arc<dyn object_store::ObjectStore>)
        .with_merge_operator(cas::Storage::merge_operator())
        .build()
        .await
        .unwrap();
    let inner: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let cas = cas::Storage::builder(db, inner.clone(), target_pack_bytes).build();
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
async fn a_full_pack_flushes_automatically_and_stays_readable() {
    // Small enough that a single chunk's write crosses the threshold.
    let (cas, inner) = packing_cas(8).await;

    let content = cas::Bytes::from_static(b"0123456789");
    let d = cas.put(&content).await.unwrap();

    assert_eq!(inner.list(None).count().await, 1);
    assert_eq!(cas.get::<cas::Bytes>(&d).await.unwrap(), Some(content));
}
