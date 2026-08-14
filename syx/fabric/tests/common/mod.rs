//! Shared fixtures for `fabric`'s external test suite.
#![allow(dead_code)]

use std::path::Path;
use std::sync::Arc;

use object_store::ObjectStore;

pub fn command(program: &str, args: &[&str]) -> fabric::Command {
    fabric::Command::new(program).args(args)
}

/// A `Graph` backed by a local-filesystem `ObjectStore` rooted at `root`.
pub async fn graph(root: impl AsRef<Path>) -> fabric::Graph {
    let backend: Arc<dyn ObjectStore> =
        Arc::new(object_store::local::LocalFileSystem::new_with_prefix(root).unwrap());
    let cas = cas::Storage::builder("test", backend).build().await.unwrap();
    fabric::Graph::new(cas)
}

/// A `Graph` backed by a local temporary directory.
pub async fn temp_graph() -> (testing::TempDir, fabric::Graph) {
    let dir = testing::tempdir();
    let graph = graph(dir.path()).await;
    (dir, graph)
}
