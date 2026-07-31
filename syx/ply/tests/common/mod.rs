//! Shared fixtures for `ply`'s external test suite.
#![allow(dead_code)]

pub fn store() -> (testing::TempDir, ply::Repository) {
    let dir = testing::tempdir();
    let store = ply::Repository::open(dir.path(), 16 * 1024 * 1024).unwrap();
    (dir, store)
}
