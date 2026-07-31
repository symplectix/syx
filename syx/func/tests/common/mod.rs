//! Shared fixtures for `func`'s external test suite.
#![allow(dead_code)]

pub fn command(program: &str, args: &[&str]) -> func::Command {
    func::Command::new(program).args(args)
}

pub fn store() -> (testing::TempDir, ply::Repository) {
    let dir = testing::tempdir();
    let store = ply::Repository::open(dir.path(), 16 * 1024 * 1024).unwrap();
    (dir, store)
}
