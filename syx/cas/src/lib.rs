//! cas: content addressing -- chunking, hashing, and encoding. The
//! storage engine that puts these to use against `slatedb`/`object_store`
//! lives in `fabric` (its only consumer), not here -- see
//! `fabric::storage`.

pub use bytes::Bytes;

mod chunking;
mod codec;
mod hash;

pub use chunking::Chunking;
pub use codec::{
    Codec,
    ContentFlags,
};
pub use hash::{
    Digest,
    FromBytes,
    Hasher,
    ToBytes,
};
