//! Universe slicing bits.

use std::ops::Deref;

mod block;
mod build;
mod chunk;

#[cfg(test)]
mod block_test;
#[cfg(test)]
mod build_test;
#[cfg(test)]
mod chunk_test;

/// Sliced bits.
#[derive(Debug, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct Bits(pub(crate) [u8]);

/// Owned `Bits`.
#[derive(Debug, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct BitVec(pub(crate) Vec<u8>);

impl Bits {
    // #[inline]
    // fn new<B: ?Sized + AsRef<[u8]>>(bytes: &B) -> &Bits {
    //     Bits::from_bytes(bytes.as_ref())
    // }

    #[inline]
    const fn from_bytes(slice: &[u8]) -> &Bits {
        unsafe { &*(slice as *const [u8] as *const Bits) }
    }

    #[inline]
    const fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    // #[inline]
    // const fn as_bytes_mut(&mut self) -> &mut [u8] {
    //     &mut self.0
    // }
}

impl Deref for BitVec {
    type Target = Bits;
    #[inline]
    fn deref(&self) -> &Self::Target {
        Bits::from_bytes(self.0.as_slice())
    }
}

mod consts {
    #![allow(unused)]

    pub(crate) const CHUNK_KIND_SPARSE: u16 = 0;
    pub(crate) const CHUNK_KIND_DENSE: u16 = 1;
    pub(crate) const CHUNK_KIND_FULL: u16 = 2;

    pub(crate) const BLOCK_KIND_SPARSE: u16 = 0;
    pub(crate) const BLOCK_KIND_DENSE: u16 = 1;

    // CHUNK_BITS is `u16::MAX + 1`, so need `-1`
    // to save cardinalities as u16.
    pub(crate) const CHUNK_BITS: u64 = 1 << 16;
    // BLOCK_BITS is `u8::MAX + 1`, so need `-1`
    // to save cardinalities as u8.
    pub(crate) const BLOCK_BITS: u64 = 1 << 8;

    // Where the universe value `u` is `1<<32`.
    pub(crate) const MAX_CHUNKS: usize = (1 << 32) / CHUNK_BITS as usize;

    pub(crate) const MAX_BLOCKS: usize = (CHUNK_BITS / BLOCK_BITS) as usize;

    // The maximum number of bytes for each chunk.
    pub(crate) const MAX_CHUNK_BYTES: usize = DENSE_CHUNK_BYTES;

    pub(crate) const DENSE_CHUNK_BYTES: usize = (CHUNK_BITS / u8::BITS as u64) as usize;
    pub(crate) const DENSE_BLOCK_BYTES: usize = (BLOCK_BITS / u8::BITS as u64) as usize;

    pub(crate) const SPARSE_CHUNK_THRESHOLD: u16 = (CHUNK_BITS / 2) as u16;
    pub(crate) const SPARSE_BLOCK_THRESHOLD: u64 = BLOCK_BITS / 8;

    // HEADER1 contains metadata for each non-empty chunk
    // - the index
    // - the number of set bits
    // - the number of bytes
    // - the chunk kind and the number of blocks
    // Each of these values should fit into u16.
    pub(crate) const HEADER1_BYTES: usize = 8;

    // HEADER2 contains metadata for each non-empty block
    // - the index
    // - the number of set bits
    // Each of these values should fit into u8.
    pub(crate) const HEADER2_BYTES: usize = 2;

    // Chunks are represented as [u16], and CHUNK_U16_LEN is the length
    // of a single slice per chunk.
    pub(crate) const CHUNK_U16_LEN: usize = HEADER1_BYTES / (u16::BITS / u8::BITS) as usize;

    // Blocks are represented as [u64], and BLOCK_U64_LEN is the length
    // of a single slice per block.
    pub(crate) const BLOCK_U64_LEN: usize = (BLOCK_BITS / u64::BITS as u64) as usize;

    pub(crate) const BLOCK_INFO_SIZE: usize = 2;
}
