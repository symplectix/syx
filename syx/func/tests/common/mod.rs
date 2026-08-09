//! Shared fixtures for `func`'s external test suite.
#![allow(dead_code)]

use std::sync::Arc;

use object_store::ObjectStore;
use slatedb::Db;

pub fn command(program: &str, args: &[&str]) -> func::Command {
    func::Command::new(program).args(args)
}

pub async fn store() -> (testing::TempDir, ply::Repository) {
    let dir = testing::tempdir();
    let backend: Arc<dyn ObjectStore> =
        Arc::new(object_store::local::LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let db = Db::builder("test", backend.clone())
        .with_merge_operator(cas::Storage::merge_operator())
        .build()
        .await
        .unwrap();
    let cas = cas::Storage::builder(db, backend, 1024 * 1024).build();
    let store = ply::Repository::new(cas);
    (dir, store)
}
