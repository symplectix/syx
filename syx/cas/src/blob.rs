//! One trait per blob operation, so a backend can implement only the
//! ones it actually supports.

use std::io;

use bytes::Bytes;

use crate::hash::Digest;

/// Whether a blob is stored, without fetching it.
pub trait Exists: Sync {
    /// Whether `key` is already stored, without fetching its value.
    fn contains_blob(&self, key: Digest) -> impl Future<Output = io::Result<bool>> + Send;
}

/// Fetch a blob by its key.
pub trait Get: Sync {
    /// Fetch bytes stored under `key`, if present.
    fn get_blob(&self, key: Digest) -> impl Future<Output = io::Result<Option<Bytes>>> + Send;
}

/// Store a blob under a key.
pub trait Put: Sync {
    /// Store `bytes` under `key`.
    fn put_blob(&self, key: Digest, bytes: Bytes) -> impl Future<Output = io::Result<()>> + Send;
}
