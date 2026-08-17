//! Round-trips content through `fabric::Graph` backed by a real (local)
//! S3-compatible remote, including a blob large enough to span multiple
//! chunks (and so multiple staged entries plus a manifest), and confirms
//! staged entries do consolidate into pack objects.
#![cfg(all(target_os = "linux", target_arch = "x86_64"))]

use std::sync::Arc;

use aws_sdk_s3::config::{
    BehaviorVersion,
    Credentials,
    Region,
};
use content_addressing as cas;
use futures::StreamExt as _;
use object_store::ObjectStore;
use object_store::aws::AmazonS3Builder;
use object_store::memory::InMemory;
use testing::s3;

const BUCKET: &str = "cas-test";

/// `fabric::Graph` backed by a real `AmazonS3`-compatible remote against
/// `s3_server`, with `BUCKET` created and ready. Also returns that same
/// remote as an `Arc<dyn ObjectStore>`, for tests that need to inspect
/// pack objects directly.
async fn s3_graph(
    s3_server: &s3::Server,
    packs_threshold: u64,
) -> (fabric::Graph, Arc<dyn ObjectStore>) {
    // A region is required by both clients below, but this server
    // doesn't validate it. "us-east-1" is just a conventional value.
    let s3_client = aws_sdk_s3::Client::from_conf(
        aws_sdk_s3::config::Builder::new()
            .behavior_version(BehaviorVersion::latest())
            .endpoint_url(s3_server.endpoint())
            .region(Region::new("us-east-1"))
            .credentials_provider(Credentials::new(
                s3::ACCESS_KEY_ID,
                s3::SECRET_ACCESS_KEY,
                None,
                None,
                "test",
            ))
            // Without this, the client addresses buckets as a subdomain of the
            // endpoint (e.g. "bucket.127.0.0.1:<port>"), which can't resolve.
            // Path-style puts the bucket in the URL path instead.
            .force_path_style(true)
            .build(),
    );
    s3_client.create_bucket().bucket(BUCKET).send().await.unwrap();

    let remote: Arc<dyn ObjectStore> = Arc::new(
        AmazonS3Builder::new()
            .with_endpoint(s3_server.endpoint())
            .with_region("us-east-1")
            .with_bucket_name(BUCKET)
            .with_access_key_id(s3::ACCESS_KEY_ID)
            .with_secret_access_key(s3::SECRET_ACCESS_KEY)
            .with_allow_http(true)
            .build()
            .unwrap(),
    );

    let db_backend: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    // Leaked, not returned: `Staging` keeps writing into this directory
    // for as long as `graph` lives, and these tests never outlive the
    // process, so there's nothing to clean up on drop that matters here.
    let staging_dir = testing::tempdir().keep();
    let graph = fabric::Graph::builder("test", db_backend, staging_dir)
        .packs(remote.clone())
        .packs_threshold(packs_threshold)
        .build()
        .await
        .unwrap();
    (graph, remote)
}

#[tokio::test]
async fn get_returns_what_was_put() {
    let s3_server = s3::Server::spawn(testing::tempdir()).unwrap();
    let (graph, _remote) = s3_graph(&s3_server, 1024 * 1024).await;

    let content = cas::Bytes::from_static(b"hello");
    let d = graph.cas().put(&content).await.unwrap();
    assert_eq!(graph.cas().get::<cas::Bytes>(&d).await.unwrap(), Some(content));

    let content = cas::Bytes::from(testing::random_bytes(2 * 1024 * 1024));
    let d = graph.cas().put(&content).await.unwrap();
    assert_eq!(graph.cas().get::<cas::Bytes>(&d).await.unwrap(), Some(content));
}

#[tokio::test]
async fn crossing_the_threshold_eventually_flushes_and_stays_readable() {
    let s3_server = s3::Server::spawn(testing::tempdir()).unwrap();
    // Small enough that a single chunk's write crosses the threshold.
    let (graph, remote) = s3_graph(&s3_server, 8).await;

    // Crossing `packs_threshold` triggers `flush_pending` in the
    // background rather than waiting on it, so the pack shows up on
    // `remote` at some point after this returns, not necessarily before
    // it does. Content stays readable throughout either way, since `get`
    // checks the still-staged copy first and falls through to the pack
    // once it exists.
    let v1 = cas::Bytes::from_static(b"abcdefg1");
    let d1 = graph.cas().put(&v1).await.unwrap();
    let v2 = cas::Bytes::from_static(b"abcdefg2");
    let d2 = graph.cas().put(&v2).await.unwrap();

    for _ in 0..100 {
        if remote.list(None).count().await > 0 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(remote.list(None).count().await > 0, "pack never showed up on remote");

    assert_eq!(graph.cas().get::<cas::Bytes>(&d1).await.unwrap(), Some(v1));
    assert_eq!(graph.cas().get::<cas::Bytes>(&d2).await.unwrap(), Some(v2));
}

#[tokio::test]
async fn content_in_different_packs_stays_independently_readable() {
    let s3_server = s3::Server::spawn(testing::tempdir()).unwrap();
    // Large enough that nothing auto-flushes on its own -- each pack
    // below is built up by staging several entries, then flushed
    // explicitly, so it ends up holding more than just one entry.
    let (graph, remote) = s3_graph(&s3_server, 1024 * 1024).await;

    async fn put_and_flush(graph: &fabric::Graph, values: &[cas::Bytes]) -> Vec<cas::Digest> {
        let mut digests = Vec::with_capacity(values.len());
        for v in values {
            digests.push(graph.cas().put(v).await.unwrap());
        }
        graph.cas().flush_pending().await.unwrap();
        digests
    }

    let pack_a = [
        cas::Bytes::from_static(b"pack-a-value-1"),
        cas::Bytes::from_static(b"pack-a-value-2"),
        cas::Bytes::from_static(b"pack-a-value-3"),
    ];
    let pack_a_digests = put_and_flush(&graph, &pack_a).await;
    assert_eq!(remote.list(None).count().await, 1);

    let pack_b = [
        cas::Bytes::from_static(b"pack-b-value-1"),
        cas::Bytes::from_static(b"pack-b-value-2"),
        cas::Bytes::from_static(b"pack-b-value-3"),
    ];
    let pack_b_digests = put_and_flush(&graph, &pack_b).await;
    assert_eq!(remote.list(None).count().await, 2);

    let entries = pack_a.iter().zip(&pack_a_digests).chain(pack_b.iter().zip(&pack_b_digests));
    for (v, d) in entries {
        assert_eq!(graph.cas().get::<cas::Bytes>(d).await.unwrap(), Some(v.clone()));
    }
}
