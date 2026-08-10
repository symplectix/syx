//! Round-trips content through `cas::Storage` backed by a real (local)
//! S3-compatible remote.
#![cfg(all(target_os = "linux", target_arch = "x86_64"))]

use std::sync::Arc;

use aws_sdk_s3::config::{
    BehaviorVersion,
    Credentials,
    Region,
};
use object_store::aws::AmazonS3Builder;
use object_store::memory::InMemory;
use testing::s3;

const BUCKET: &str = "cas-test";

/// `cas::Storage` backed by a real `AmazonS3`-compatible remote against
/// `s3_server`, with `BUCKET` created and ready.
async fn s3_cas(s3_server: &s3::Server) -> cas::Storage {
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

    let remote = AmazonS3Builder::new()
        .with_endpoint(s3_server.endpoint())
        .with_region("us-east-1")
        .with_bucket_name(BUCKET)
        .with_access_key_id(s3::ACCESS_KEY_ID)
        .with_secret_access_key(s3::SECRET_ACCESS_KEY)
        .with_allow_http(true)
        .build()
        .unwrap();

    let db_backend: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    cas::Storage::builder("test", db_backend).packs(Arc::new(remote)).build().await.unwrap()
}

#[tokio::test]
async fn get_returns_what_was_put() {
    let s3_server = s3::Server::spawn(testing::tempdir()).unwrap();
    let cas = s3_cas(&s3_server).await;

    let content = cas::Bytes::from_static(b"hello");
    let d = cas.put(&content).await.unwrap();

    assert_eq!(cas.get::<cas::Bytes>(&d).await.unwrap(), Some(content));
}
