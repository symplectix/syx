//! A `Store` round-trips large content correctly regardless of whether
//! it ends up compressed. (Whether compression was actually applied is
//! an internal decision, covered by unit tests inside `cas::store`.)

mod common;
use common::temp_graph;

#[tokio::test]
async fn large_incompressible_content_streamed_via_copy_from_round_trips() {
    // Exercises copy_from's non-seekable, streaming branch (content
    // over its inline threshold).
    let (_dir, store) = temp_graph().await;
    let content = testing::random_bytes(100_000);
    let mut cursor = std::io::Cursor::new(content.clone());
    let d = store.copy_from(content.len() as u64, &mut cursor).await.unwrap();
    assert_eq!(store.get(&d).await.unwrap(), Some(cas::Bytes::from(content)));
}

#[tokio::test]
async fn large_compressible_content_streamed_via_copy_from_round_trips() {
    let (_dir, store) = temp_graph().await;
    let content = vec![b'a'; 100_000];
    let mut cursor = std::io::Cursor::new(content.clone());
    let d = store.copy_from(content.len() as u64, &mut cursor).await.unwrap();
    assert_eq!(store.get(&d).await.unwrap(), Some(cas::Bytes::from(content)));
}
