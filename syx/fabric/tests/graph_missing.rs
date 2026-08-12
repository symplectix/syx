//! A digest that was never stored is reported as absent, not an error.

mod common;
use common::temp_graph;

#[tokio::test]
async fn get_missing_digest_is_none() {
    let (_dir, graph) = temp_graph().await;
    assert_eq!(
        graph.get::<cas::Bytes>(&cas_testing::digest_bytes(b"missing")).await.unwrap(),
        None
    );
}
