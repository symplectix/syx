//! `Store::read_into` streams content to a writer instead of returning
//! it as a fully materialized value.

mod common;
use std::io;

use common::{
    digest_bytes,
    store,
};

#[tokio::test]
async fn read_into_writes_small_content() {
    let (_dir, store) = store();
    let d = store.put(&cas::Bytes::from_static(b"hello")).await.unwrap();

    let mut out = Vec::new();
    let found = store.read_into(&d, &mut out).await.unwrap();
    assert!(found);
    assert_eq!(out, b"hello");
}

#[tokio::test]
async fn read_into_writes_large_multi_chunk_content() {
    let (_dir, store) = store();
    let content = testing::random_bytes(600_000);
    let d = store.put(&cas::Bytes::from(content.clone())).await.unwrap();

    let mut out = Vec::new();
    let found = store.read_into(&d, &mut out).await.unwrap();
    assert!(found);
    assert_eq!(out, content);
}

#[tokio::test]
async fn read_into_matches_get_for_the_same_digest() {
    let (_dir, store) = store();
    let content = testing::random_bytes(600_000);
    let d = store.copy_from(content.len() as u64, &mut io::Cursor::new(content)).await.unwrap();

    let from_get = store.get::<cas::Bytes>(&d).await.unwrap().unwrap();
    let mut from_read_into = Vec::new();
    store.read_into(&d, &mut from_read_into).await.unwrap();

    assert_eq!(from_read_into, from_get.to_vec());
}

#[tokio::test]
async fn read_into_returns_false_for_a_missing_digest() {
    let (_dir, store) = store();
    let missing = digest_bytes(b"never stored");

    let mut out = Vec::new();
    let found = store.read_into(&missing, &mut out).await.unwrap();
    assert!(!found);
    assert!(out.is_empty());
}
