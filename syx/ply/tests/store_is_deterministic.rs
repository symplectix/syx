//! A `Store`'s returned digest doesn't depend on which method ingested
//! the content.

mod common;
use common::store;

#[tokio::test]
async fn put_returns_the_content_digest() {
    let (_dir, store) = store().await;
    let d = store.put(&cas::Bytes::from_static(b"hello")).await.unwrap();
    assert_eq!(d, cas_testing::digest_bytes(b"hello"));
}

#[tokio::test]
async fn copy_from_file_produces_the_same_digest_as_put() {
    let (_dir, store) = store().await;
    let src_dir = testing::tempdir();
    let src = src_dir.path().join("blob");
    std::fs::write(&src, b"hello").unwrap();

    let mut file = tokio::fs::File::open(&src).await.unwrap();
    let len = file.metadata().await.unwrap().len();
    let from_reader = store.copy_from(len, &mut file).await.unwrap();
    let from_bytes = store.put(&cas::Bytes::from_static(b"hello")).await.unwrap();
    assert_eq!(from_reader, from_bytes);
}

#[tokio::test]
async fn copy_from_bytes_produces_the_same_digest_as_put() {
    let (_dir, store) = store().await;
    let content = cas::Bytes::from_static(b"hello");
    let mut cursor = std::io::Cursor::new(&content);
    let d = store.copy_from(content.len() as u64, &mut cursor).await.unwrap();
    assert_eq!(d, cas_testing::digest_bytes(b"hello"));
    assert_eq!(store.get(&d).await.unwrap(), Some(content));
}
