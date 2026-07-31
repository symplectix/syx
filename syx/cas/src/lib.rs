//! cas: content-addressed storage.

pub use bytes::Bytes;
pub use hash::{
    Digest,
    FromBytes,
    Hasher,
    ToBytes,
    digest,
};
pub use storage::Storage;

pub mod blob;
mod hash;
mod storage;
