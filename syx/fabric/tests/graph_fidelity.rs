//! Content stored in a `Store` is preserved faithfully.

use content_addressing as cas;

mod common;
use common::temp_graph;

#[tokio::test]
async fn content_put_is_returned_unchanged_by_get() {
    let (_dir, graph) = temp_graph().await;
    let d = graph.cas().put(&cas::Bytes::from_static(b"hello")).await.unwrap();
    assert_eq!(graph.cas().get(&d).await.unwrap(), Some(cas::Bytes::from_static(b"hello")));
}

#[tokio::test]
async fn copy_from_accepts_a_file_and_streams_it_in() {
    // A file already on disk (not just in-memory bytes) can be
    // ingested via copy_from, streamed in without requiring the
    // caller to load it into memory first.
    let (_dir, graph) = temp_graph().await;
    let src_dir = testing::tempdir();
    let src = src_dir.path().join("blob");
    std::fs::write(&src, b"hello").unwrap();

    let mut file = tokio::fs::File::open(&src).await.unwrap();
    let len = file.metadata().await.unwrap().len();
    let d = graph.cas().copy_from(len, &mut file).await.unwrap();
    assert_eq!(d, cas_testing::digest_bytes(b"hello"));
    assert_eq!(graph.cas().get(&d).await.unwrap(), Some(cas::Bytes::from_static(b"hello")));
}
