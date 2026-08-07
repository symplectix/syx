//! Bridges `ObjectStore` to `cas::blob::{Exists, Get, Put}`.

use std::io;
use std::sync::Arc;

use cas::Bytes;
use object_store::path::{
    Path as ObjectStorePath,
    PathPart,
};
use object_store::{
    ObjectStore,
    ObjectStoreExt as _,
};

/// Stores each blob as one object, keyed by a hex-sharded path derived
/// from its `Digest`.
pub struct Adapter {
    inner: Arc<dyn ObjectStore>,
}

impl Adapter {
    /// Wraps `inner`.
    pub fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self { inner }
    }
}

impl cas::blob::Exists for Adapter {
    async fn contains_blob(&self, key: cas::Digest) -> io::Result<bool> {
        match self.inner.head(&path(key)).await {
            Ok(_) => Ok(true),
            Err(e) => {
                let e = io::Error::from(e);
                if e.kind() == io::ErrorKind::NotFound { Ok(false) } else { Err(e) }
            }
        }
    }
}

impl cas::blob::Get for Adapter {
    async fn get_blob(&self, key: cas::Digest) -> io::Result<Option<Bytes>> {
        match self.inner.get(&path(key)).await {
            Ok(result) => Ok(Some(result.bytes().await?)),
            Err(e) => {
                let e = io::Error::from(e);
                if e.kind() == io::ErrorKind::NotFound { Ok(None) } else { Err(e) }
            }
        }
    }
}

impl cas::blob::Put for Adapter {
    async fn put_blob(&self, key: cas::Digest, bytes: Bytes) -> io::Result<()> {
        self.inner.put(&path(key), bytes.into()).await?;
        Ok(())
    }
}

/// Leading two-character segments to shard blob paths by, before the rest
/// of the hex digest as a final segment.
const SHARD_DEPTH: usize = 1;

fn path(key: cas::Digest) -> ObjectStorePath {
    let hex = format!("{key:x}");
    ObjectStorePath::from_iter(hex_parts(&hex, SHARD_DEPTH))
}

/// `depth` leading two-character segments of `hex`, then the rest, as
/// `PathPart`s. For example, depth=3 yields "ab", "cd", "ef", "<remaining
/// 58 hex chars>". `depth` must be less than half of `hex`'s length.
///
/// Every part here is plain hex (`0-9a-f`), none of which needs percent
/// escaping, so `PathPart::from(&str)` borrows straight from `hex` --
/// this doesn't allocate beyond `hex` itself.
fn hex_parts(hex: &str, depth: usize) -> impl Iterator<Item = PathPart<'_>> {
    assert!(depth * 2 < hex.len(), "depth must be less than half of hex's length");
    (0..depth)
        .map(|i| PathPart::from(&hex[i * 2..i * 2 + 2]))
        .chain([PathPart::from(&hex[depth * 2..])])
}
