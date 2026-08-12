//! Content much larger than a single chunk still round-trips exactly,
//! and every ingestion method agrees on its digest.

mod common;
use common::temp_graph;

/// Comfortably larger than the largest possible single chunk, so this
/// is guaranteed to produce more than one chunk regardless of exactly
/// where fastcdc happens to cut it.
const LARGE: usize = 5_000_000;

#[tokio::test]
async fn large_content_via_put_round_trips() {
    let (_dir, graph) = temp_graph().await;
    let content = testing::random_bytes(LARGE);
    let d = graph.put(&cas::Bytes::from(content.clone())).await.unwrap();
    assert_eq!(graph.get(&d).await.unwrap(), Some(cas::Bytes::from(content)));
}

#[tokio::test]
async fn large_content_via_copy_from_round_trips() {
    let (_dir, graph) = temp_graph().await;
    let content = testing::random_bytes(LARGE);
    let mut cursor = std::io::Cursor::new(content.clone());
    let d = graph.copy_from(content.len() as u64, &mut cursor).await.unwrap();
    assert_eq!(graph.get(&d).await.unwrap(), Some(cas::Bytes::from(content)));
}

#[tokio::test]
async fn put_and_copy_from_agree_on_digest_for_large_content() {
    let (_dir, graph) = temp_graph().await;
    let content = testing::random_bytes(LARGE);

    let from_put = graph.put(&cas::Bytes::from(content.clone())).await.unwrap();

    let len = content.len() as u64;
    let mut cursor = std::io::Cursor::new(content);
    let from_copy = graph.copy_from(len, &mut cursor).await.unwrap();

    assert_eq!(from_put, from_copy);
}
