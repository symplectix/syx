//! Test helpers for `cas::Digest`.

use cas::{
    Digest,
    Hasher,
    ToBytes,
};

/// Digest of `bytes`.
pub fn digest_bytes(bytes: &[u8]) -> Digest {
    let mut h = Hasher::new();
    h.part(bytes);
    h.digest()
}

/// Digest of `parts`, combined in order.
pub fn digest_parts<I, T>(parts: I) -> Digest
where
    I: IntoIterator<Item = T>,
    T: AsRef<[u8]>,
{
    let mut h = Hasher::new();
    h.parts(parts);
    h.digest()
}

/// Digest of `value`'s canonical byte encoding, unwrapped: every
/// `ToBytes` impl used in tests is expected to succeed, so tests don't
/// need to handle the error case.
pub fn digest<T: ToBytes>(value: &T) -> Digest {
    let bytes = value.to_bytes().unwrap();
    let mut h = Hasher::new();
    h.part(bytes);
    h.digest()
}
