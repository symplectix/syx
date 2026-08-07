//! `Adapter`'s behavior against an in-memory `ObjectStore`.

use std::sync::Arc;

use cas::blob::{
    Exists as _,
    Get as _,
    Put as _,
};

fn adapter() -> cas_object_store::Adapter {
    cas_object_store::Adapter::new(Arc::new(object_store::memory::InMemory::new()))
}

#[tokio::test]
async fn missing_digest_does_not_exist() {
    let store = adapter();
    let digest = cas_testing::digest_bytes(b"missing");
    assert!(!store.contains_blob(digest).await.unwrap());
}

#[tokio::test]
async fn missing_digest_returns_none() {
    let store = adapter();
    let digest = cas_testing::digest_bytes(b"missing");
    assert_eq!(store.get_blob(digest).await.unwrap(), None);
}

#[tokio::test]
async fn put_blob_round_trips_through_get_blob() {
    let store = adapter();
    let digest = cas_testing::digest_bytes(b"hello");
    store.put_blob(digest, cas::Bytes::from_static(b"hello")).await.unwrap();
    assert_eq!(store.get_blob(digest).await.unwrap(), Some(cas::Bytes::from_static(b"hello")));
}

#[tokio::test]
async fn put_blob_makes_contains_blob_true() {
    let store = adapter();
    let digest = cas_testing::digest_bytes(b"hello");
    store.put_blob(digest, cas::Bytes::from_static(b"hello")).await.unwrap();
    assert!(store.contains_blob(digest).await.unwrap());
}

#[tokio::test]
async fn different_digests_are_independent() {
    let store = adapter();
    let a = cas_testing::digest_bytes(b"a");
    let b = cas_testing::digest_bytes(b"b");
    store.put_blob(a, cas::Bytes::from_static(b"a")).await.unwrap();
    assert_eq!(store.get_blob(b).await.unwrap(), None);
}
