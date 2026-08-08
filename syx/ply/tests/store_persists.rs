//! What a `Repository` persists across instances.

mod common;

#[tokio::test]
async fn content_persists_across_store_instances() {
    // A fresh Repository instance over the same root sees content a prior
    // instance wrote: proof it actually landed in the backing store.
    let dir = testing::tempdir();

    let writer = common::repository(dir.path());
    let d = writer.put(&cas::Bytes::from_static(b"hello")).await.unwrap();
    // Drop `writer` before reopening the same root, so the two instances
    // stay sequential rather than coexisting live.
    drop(writer);

    let reader = common::repository(dir.path());
    assert_eq!(reader.get(&d).await.unwrap(), Some(cas::Bytes::from_static(b"hello")));
}
