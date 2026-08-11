//! Shared fixtures for `ply`'s external test suite.
#![allow(dead_code)]

use std::path::Path;
use std::sync::Arc;

use object_store::ObjectStore;

/// A `Repository` backed by a local-filesystem `ObjectStore` rooted at
/// `root`, staged through a `slatedb` sharing the same root -- so a
/// fresh instance over the same `root` sees everything a prior one
/// wrote, staged or already packed alike.
pub async fn repository(root: impl AsRef<Path>) -> ply::Repository {
    let backend: Arc<dyn ObjectStore> =
        Arc::new(object_store::local::LocalFileSystem::new_with_prefix(root).unwrap());
    let cas = cas::Storage::builder("test", backend).build().await.unwrap();
    ply::Repository::new(cas)
}

pub async fn store() -> (testing::TempDir, ply::Repository) {
    let dir = testing::tempdir();
    let store = repository(dir.path()).await;
    (dir, store)
}
