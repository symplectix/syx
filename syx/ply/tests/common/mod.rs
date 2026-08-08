//! Shared fixtures for `ply`'s external test suite.
#![allow(dead_code)]

use std::path::Path;
use std::sync::Arc;

/// A `Repository` backed by a plain local-filesystem `ObjectStore` rooted at `root`.
pub fn repository(root: impl AsRef<Path>) -> ply::Repository {
    let backend = object_store::local::LocalFileSystem::new_with_prefix(root).unwrap();
    ply::Repository::new(cas::Storage::new(Arc::new(backend)))
}

pub fn store() -> (testing::TempDir, ply::Repository) {
    let dir = testing::tempdir();
    let store = repository(dir.path());
    (dir, store)
}
