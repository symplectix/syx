//! The digest primitive everything else in `cas` is addressed by.

use std::fmt;

use bytes::Bytes;
use sha2::Digest as _;

/// This value's canonical byte encoding.
///
/// Implementations must keep this consistent with `FromBytes`:
/// `T::from_bytes(&x.to_bytes())` must equal `Ok(x)`, and encoding the
/// same logical value twice (in any order it was built) must always
/// produce identical bytes.
pub trait ToBytes {
    /// Why encoding can fail.
    type Error: fmt::Debug;

    /// Encode `self` into its canonical byte form.
    fn to_bytes(&self) -> Result<Bytes, Self::Error>;
}

/// The inverse of `ToBytes`.
/// Build a value back from its own byte encoding.
pub trait FromBytes: Sized {
    /// Why decoding can fail.
    type Error: fmt::Debug;

    /// Decode `bytes` back into `Self`.
    fn from_bytes(bytes: Bytes) -> Result<Self, Self::Error>;
}

impl ToBytes for Bytes {
    type Error = std::convert::Infallible;
    fn to_bytes(&self) -> Result<Bytes, Self::Error> {
        Ok(self.clone())
    }
}

impl FromBytes for Bytes {
    type Error = std::convert::Infallible;
    fn from_bytes(bytes: Bytes) -> Result<Self, Self::Error> {
        Ok(bytes)
    }
}

/// A digest's raw bytes.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct Digest(#[serde(with = "serde_bytes")] [u8; 32]);

impl Digest {
    /// Wrap already-computed digest bytes.
    pub fn new(bytes: [u8; 32]) -> Self {
        Digest(bytes)
    }
}

impl AsRef<[u8]> for Digest {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl fmt::LowerHex for Digest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for b in &self.0 {
            write!(f, "{b:02x}")?;
        }
        Ok(())
    }
}

/// Builds a length-prefixed `Digest` over an ordered sequence of parts.
///
/// This framing is self-delimiting, so no two distinct sequences
/// of parts produce the same digest. For example, `part(b"a").part(b"b")`
/// cannot collide with `part(b"ab")`.
pub struct Hasher {
    hasher: sha2::Sha256,
}

impl Default for Hasher {
    fn default() -> Self {
        Self::new()
    }
}

impl Hasher {
    /// A fresh `Hasher` with no parts folded in yet.
    pub fn new() -> Self {
        Hasher { hasher: sha2::Sha256::new() }
    }

    /// Fold one more part into the digest.
    pub fn part(&mut self, part: impl AsRef<[u8]>) -> &mut Self {
        let bytes = part.as_ref();
        self.hasher.update((bytes.len() as u64).to_be_bytes());
        self.hasher.update(bytes);
        self
    }

    /// Fold each part into the digest, in order.
    pub fn parts<I, T>(&mut self, parts: I) -> &mut Self
    where
        I: IntoIterator<Item = T>,
        T: AsRef<[u8]>,
    {
        for part in parts {
            self.part(part);
        }
        self
    }

    /// Finalize and return the digest's bytes, resetting so `self` can
    /// be reused to build another digest from scratch.
    pub fn digest(&mut self) -> Digest {
        Digest(self.hasher.finalize_reset().into())
    }
}
