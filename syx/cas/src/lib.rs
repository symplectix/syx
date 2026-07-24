//! cas: content-addressed storage.

mod blob;
mod hash;

pub use blob::{
    Storage,
    copy_from,
    get,
    put,
    read_into,
};
pub use bytes::Bytes;
pub use hash::{
    Digest,
    FromBytes,
    Hasher,
    ToBytes,
    digest,
};
