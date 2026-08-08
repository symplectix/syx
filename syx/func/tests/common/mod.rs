//! Shared fixtures for `func`'s external test suite.
#![allow(dead_code)]

use std::sync::Arc;

pub fn command(program: &str, args: &[&str]) -> func::Command {
    func::Command::new(program).args(args)
}

pub fn store() -> (testing::TempDir, ply::Repository) {
    let dir = testing::tempdir();
    let backend = object_store::local::LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let store = ply::Repository::new(cas::Storage::new(Arc::new(backend)));
    (dir, store)
}
