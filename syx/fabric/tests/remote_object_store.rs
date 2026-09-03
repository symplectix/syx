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
    flush_threshold: u64,
) -> (fabric::Graph, Arc<dyn ObjectStore>) {
    s3_graph_with(s3_server, flush_threshold, None).await
}

/// Like `s3_graph`, but also lets a test override `max_forgetter_duration`
/// (left at the crate's own default when `None`).
async fn s3_graph_with(
    s3_server: &s3::Server,
    flush_threshold: u64,
    max_forgetter_duration: Option<std::time::Duration>,
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
    // Leaked, not returned: `Forgetter` keeps writing into this directory
    // for as long as `graph` lives, and these tests never outlive the
    // process, so there's nothing to clean up on drop that matters here.
    let forgetter_dir = testing::tempdir().keep();
    let mut builder = fabric::Graph::builder(forgetter_dir)
        .db_prefix("test")
        .db_backend(db_backend)
        .blobs(remote.clone())
        .flush_threshold(flush_threshold);
    if let Some(d) = max_forgetter_duration {
        builder = builder.max_forgetter_duration(d);
    }
    let graph = builder.build().await.unwrap();
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
async fn idle_content_eventually_gets_packed_without_further_activity() {
    let s3_server = s3::Server::spawn(testing::tempdir()).unwrap();
    // `flush_threshold` large enough that this one small `put` never
    // crosses it, and no further `put` ever happens after it: the only
    // thing that can ever rotate this segment out is `forgetter`'s own
    // `max_forgetter_duration` timer, which then wakes `Graph`'s
    // background flush loop to pack it.
    let (graph, remote) =
        s3_graph_with(&s3_server, 1024 * 1024, Some(std::time::Duration::from_millis(20))).await;

    let content = cas::Bytes::from_static(b"idle content");
    let d = graph.cas().put(&content).await.unwrap();

    for _ in 0..100 {
        if remote.list(None).count().await > 0 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert!(remote.list(None).count().await > 0, "background flush loop never packed idle content");
    assert_eq!(graph.cas().get::<cas::Bytes>(&d).await.unwrap(), Some(content));
}

#[tokio::test]
async fn crossing_the_threshold_eventually_flushes_and_stays_readable() {
    let s3_server = s3::Server::spawn(testing::tempdir()).unwrap();
    // Small enough that a single chunk's write crosses the threshold.
    let (graph, remote) = s3_graph(&s3_server, 8).await;

    // Crossing `flush_threshold` triggers `flush_pending` in the
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
    // Small enough that every value below crosses it on its own, so
    // `forgetter` rotates (and each of these ends up in its own pack)
    // without needing to batch several `put`s together first: grouping
    // several entries into one pack via an explicit flush was never a
    // feature `flush_pending` promised, just an incidental effect of an
    // earlier design where it forced a rotation itself. `flush_pending`
    // deliberately doesn't do that, so this test no longer controls
    // which values land in which pack.
    let (graph, remote) = s3_graph(&s3_server, 8).await;

    async fn put_all(graph: &fabric::Graph, values: &[cas::Bytes]) -> Vec<cas::Digest> {
        let mut digests = Vec::with_capacity(values.len());
        for v in values {
            digests.push(graph.cas().put(v).await.unwrap());
        }
        digests
    }

    let pack_a = [
        cas::Bytes::from_static(b"pack-a-value-1"),
        cas::Bytes::from_static(b"pack-a-value-2"),
        cas::Bytes::from_static(b"pack-a-value-3"),
    ];
    let pack_a_digests = put_all(&graph, &pack_a).await;

    let pack_b = [
        cas::Bytes::from_static(b"pack-b-value-1"),
        cas::Bytes::from_static(b"pack-b-value-2"),
        cas::Bytes::from_static(b"pack-b-value-3"),
    ];
    let pack_b_digests = put_all(&graph, &pack_b).await;

    // Each `put` above crossing the threshold rotates its own segment,
    // each of which wakes `Graph`'s background flush loop; those race
    // for `Flushing`'s claim, so most of them lose and give up without
    // retrying, leaving their segment un-packed. A single explicit
    // `flush_pending` call can lose that same race, so this retries it
    // until every value has actually landed on `remote`.
    for _ in 0..100 {
        graph.cas().flush_pending().await.unwrap();
        if remote.list(None).count().await > 1 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(remote.list(None).count().await > 1, "expected more than one pack on remote");

    let entries = pack_a.iter().zip(&pack_a_digests).chain(pack_b.iter().zip(&pack_b_digests));
    for (v, d) in entries {
        assert_eq!(graph.cas().get::<cas::Bytes>(d).await.unwrap(), Some(v.clone()));
    }
}
