//! A digest that was never stored is reported as absent, not an error.

mod common;
use common::store;

#[tokio::test]
async fn get_missing_digest_is_none() {
    let (_dir, store) = store();
    assert_eq!(
        store.get::<cas::Bytes>(&cas_testing::digest_bytes(b"missing")).await.unwrap(),
        None
    );
}
