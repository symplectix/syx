//! What a `Graph` persists across instances.

mod common;

#[tokio::test]
async fn content_persists_across_graph_instances() {
    // A fresh Graph instance over the same root sees content a prior
    // instance wrote: proof it actually landed in the backing store.
    let dir = testing::tempdir();

    let writer = common::graph(dir.path()).await;
    let d = writer.put(&cas::Bytes::from_static(b"hello")).await.unwrap();
    // Drop `writer` before reopening the same root, so the two instances
    // stay sequential rather than coexisting live.
    drop(writer);

    let reader = common::graph(dir.path()).await;
    assert_eq!(reader.get(&d).await.unwrap(), Some(cas::Bytes::from_static(b"hello")));
}
