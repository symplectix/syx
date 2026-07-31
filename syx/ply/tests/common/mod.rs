//! Shared fixtures for `ply`'s external test suite.
#![allow(dead_code)]

pub fn digest_bytes(bytes: &[u8]) -> cas::Digest {
    let mut h = cas::Hasher::new();
    h.part(bytes);
    h.digest()
}

pub fn store() -> (testing::TempDir, ply::Repository) {
    let dir = testing::tempdir();
    let store = ply::Repository::open(dir.path(), 16 * 1024 * 1024).unwrap();
    (dir, store)
}

/// `cas::digest`, unwrapped: every `ToBytes` impl used in this suite is
/// expected to succeed, so tests don't need to handle the error case.
pub fn digest<T: cas::ToBytes>(value: &T) -> cas::Digest {
    cas::digest(value).unwrap()
}
