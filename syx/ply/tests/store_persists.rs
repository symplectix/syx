//! What a `Repository` persists across instances.

mod common;

#[tokio::test]
async fn content_persists_across_store_instances() {
    // A fresh Repository instance over the same root sees content a prior
    // instance wrote: proof it actually landed in the backing store.
    let dir = testing::tempdir();

    let writer = ply::Repository::open(dir.path(), 16 * 1024 * 1024).unwrap();
    let d = writer.put(&cas::Bytes::from_static(b"hello")).await.unwrap();
    // Repository::open takes an OS advisory lock on `root`, held for as long
    // as the Repository is alive: drop `writer` before reopening the same
    // root, rather than relying on two live instances coexisting.
    drop(writer);

    let reader = ply::Repository::open(dir.path(), 16 * 1024 * 1024).unwrap();
    assert_eq!(reader.get(&d).await.unwrap(), Some(cas::Bytes::from_static(b"hello")));
}
