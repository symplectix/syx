#![allow(missing_docs, unused)]

use std::slice;

use crate::block::Blocks;
use crate::consts::*;
use crate::{
    BitVec,
    Bits,
};

impl Bits {
    #[inline]
    pub(crate) fn chunks(&self) -> Chunks<'_> {
        let (header, data) = self.split();
        Chunks { header: ChunksHeader::new(header), data }
    }

    #[inline]
    pub(crate) fn chunks_header(&self) -> ChunksHeader<'_> {
        let (header, _) = self.split();
        ChunksHeader::new(header)
    }

    fn split(&self) -> (&'_ [u16], &'_ [u8]) {
        let bytes = self.as_bytes();
        if bytes.len() >= 2 {
            // MAX_CHUNKS is 1<<16. subtract 1 to store the chunk value as u16.
            let chunks_in_bytes =
                (u16::from_le_bytes([bytes[0], bytes[1]]) as usize + 1) * HEADER1_BYTES;
            let (header, data) = bytes.split_at(2 + chunks_in_bytes);
            // Note:
            // - bytemuck::cast_slice fails if bytes are not correctly aligned.
            // - the results of casting are endian dependant.
            (bytemuck::cast_slice(&header[2..]), data)
        } else {
            (&[], &[])
        }
    }
}

impl BitVec {
    /// *Partially* constructs BitVec from the number of chunks.
    pub(crate) fn with_chunks(n: usize) -> Self {
        assert!(n <= MAX_CHUNKS);
        if n == 0 {
            BitVec(vec![])
        } else {
            let mut chunks_header_bytes = vec![0; 2 + n * 8];
            // maximum header_len is 1<<16, so need to subtract 1
            // to store the chunk value in two bytes.
            let chunks_header_len = (n - 1) as u16;
            chunks_header_bytes[..2].copy_from_slice(&chunks_header_len.to_le_bytes());
            BitVec(chunks_header_bytes)
        }
    }
}

#[derive(Debug, Clone)]
pub struct Chunks<'a> {
    pub(crate) header: ChunksHeader<'a>,
    data: &'a [u8],
}

#[derive(Debug, Clone)]
pub enum Chunk<'a> {
    Sparse { index: u32, bits: u32, blocks: Blocks<'a> },
    Dense { index: u32, bits: u32, data: &'a [u8] },
    Full { index: u32 },
}

#[derive(Debug, Clone)]
pub(crate) struct ChunksHeader<'a> {
    chunks:    slice::Iter<'a, [u16; CHUNK_U16_LEN]>,
    remainder: &'a [u16],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ChunkInfo {
    index: u32,
    bits:  u32,
    bytes: u16,
    kind:  u16,
}

impl<'a> Chunk<'a> {
    pub(crate) fn kind(&self) -> u16 {
        match self {
            Chunk::Sparse { .. } => CHUNK_KIND_SPARSE,
            Chunk::Dense { .. } => CHUNK_KIND_DENSE,
            Chunk::Full { .. } => CHUNK_KIND_FULL,
        }
    }
}

impl<'a> Iterator for Chunks<'a> {
    type Item = Chunk<'a>;
    fn next(&mut self) -> Option<Self::Item> {
        self.header.next().map(|ChunkInfo { index, bits, bytes, kind }| {
            let (data, remain) = self.data.split_at(bytes as usize);
            self.data = remain;
            match kind & 0xff {
                CHUNK_KIND_FULL => Chunk::Full { index },
                CHUNK_KIND_DENSE => Chunk::Dense { index, bits, data },
                CHUNK_KIND_SPARSE => {
                    let blocks_header_len = ((kind >> 8) as usize + 1) * 2;
                    let blocks = Blocks::new(blocks_header_len, data);
                    Chunk::Sparse { index, bits, blocks }
                }
                _ => unreachable!("bug: malformed chunk kind"),
            }
        })
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.header.size_hint()
    }
}
impl<'a> ExactSizeIterator for Chunks<'a> {}

impl<'a> ChunksHeader<'a> {
    fn new(header: &'a [u16]) -> Self {
        let (chunks, remainder) = header.as_chunks::<CHUNK_U16_LEN>();
        ChunksHeader { chunks: chunks.iter(), remainder }
    }
    pub(crate) fn remainder(&self) -> &[u16] {
        self.remainder
    }
}
impl<'a> Iterator for ChunksHeader<'a> {
    type Item = ChunkInfo;
    fn next(&mut self) -> Option<Self::Item> {
        self.chunks.next().map(|slice| ChunkInfo {
            index: slice[0] as u32,
            bits:  slice[1] as u32 + 1,
            bytes: slice[2],
            kind:  slice[3],
        })
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.chunks.size_hint()
    }
}
impl<'a> ExactSizeIterator for ChunksHeader<'a> {}
