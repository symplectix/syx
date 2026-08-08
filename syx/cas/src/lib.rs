//! cas: content-addressed storage.

pub use bytes::Bytes;
pub use hash::{
    Digest,
    FromBytes,
    Hasher,
    ToBytes,
};
pub use storage::Storage;

mod hash;
mod storage;
