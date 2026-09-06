#![allow(missing_docs, unused)]

use std::iter;

use bitcomp_core::{
    Bits as _,
    Page,
    Word,
};

use crate::consts::*;
use crate::{
    BitVec,
    Bits,
};

impl<'a> BitVec {
    pub(crate) fn build(slice: &'a [Page<[u64; 1024]>]) -> Self {
        let chunks: Vec<_> = slice.iter().enumerate().filter(|(_, b)| b.count1() > 0).collect();
        let mut bv = BitVec::with_chunks(chunks.len());

        let header: &mut [u16] =
            bytemuck::cast_slice_mut(&mut bv.0[2..2 + chunks.len() * HEADER1_BYTES]);

        let mut data: Vec<u8> = Vec::with_capacity(65536);

        for (k, (index, chunk)) in chunks.into_iter().enumerate() {
            assert!(index < 65536);
            assert!(chunk.bits() - 1 < 65536);
            let chunk = chunk.as_ref().expect("bug: chunk must be non empty");
            let index = index as u16;
            let bits = (chunk.count1() - 1) as u16;

            let (bytes, kind) = if bits == u16::MAX {
                (0, CHUNK_KIND_FULL)
            } else if bits >= SPARSE_CHUNK_THRESHOLD - 1 {
                data.extend_from_slice(bytemuck::cast_slice(chunk));
                (DENSE_CHUNK_BYTES, CHUNK_KIND_DENSE)
            } else {
                let stats = sparse_chunk_stats(chunk);
                let bytes = stats.bytes();
                // Even if the number of set bits is below the threshold, if the encoded result has
                // a byte size equal to or greater than a dense chunk, it is encoded as a dense
                // chunk.
                if bytes >= DENSE_CHUNK_BYTES {
                    data.extend_from_slice(bytemuck::cast_slice(chunk));
                    (DENSE_CHUNK_BYTES, CHUNK_KIND_DENSE)
                } else {
                    // the maximum value of stats.nblocks() is 256, so need to subtract 1
                    // to store the chunk value in a single byte.
                    let nblocks = stats.nblocks() - 1;
                    assert!(nblocks < 256);
                    let kind = (nblocks << 8) as u16 | CHUNK_KIND_SPARSE;
                    data.extend_from_slice(&stats.header);
                    data.extend_from_slice(&stats.data);
                    (bytes, kind)
                }
            };

            assert!(bytes <= DENSE_CHUNK_BYTES);
            let k = k * 4;
            header[k] = index;
            header[k + 1] = bits;
            header[k + 2] = bytes as u16;
            header[k + 3] = kind;
        }

        bv.0.append(&mut data);
        bv
    }
}

#[derive(Debug, Clone)]
pub(crate) struct SparseChunkStats {
    pub(crate) header: Vec<u8>,
    pub(crate) data:   Vec<u8>,

    pub(crate) empty_blocks:  usize,
    pub(crate) dense_blocks:  usize,
    pub(crate) sparse_blocks: usize,
}

impl Default for SparseChunkStats {
    fn default() -> Self {
        SparseChunkStats {
            header: Vec::with_capacity(MAX_BLOCKS * 2),
            data:   Vec::with_capacity(MAX_BLOCKS * 32),

            empty_blocks:  0,
            dense_blocks:  0,
            sparse_blocks: 0,
        }
    }
}

impl SparseChunkStats {
    #[inline]
    pub(crate) fn nblocks(&self) -> usize {
        self.dense_blocks + self.sparse_blocks
    }

    #[inline]
    pub(crate) fn bits(&self) -> u32 {
        self.header.as_chunks::<HEADER2_BYTES>().0.iter().map(|c| c[1] as u32 + 1).sum()
    }

    #[inline]
    pub(crate) fn bytes(&self) -> usize {
        self.header.len() + self.data.len()
    }
}

pub(crate) fn sparse_chunk_stats(chunk: &[u64]) -> SparseChunkStats {
    let (blocks, remainder) = chunk.as_chunks::<BLOCK_U64_LEN>();
    let mut stats = SparseChunkStats::default();

    for (i, block) in blocks.iter().enumerate() {
        let bits = block.count1();
        if bits == 0 {
            stats.empty_blocks += 1;
            continue;
        }

        assert!(i < 256 && bits <= 256);
        stats.header.push(i as u8);
        stats.header.push((bits - 1) as u8);

        if bits >= SPARSE_BLOCK_THRESHOLD {
            // HEADER2_BYTES + DENSE_BLOCK_BYTES
            stats.dense_blocks += 1;
            stats.data.extend_from_slice(bytemuck::cast_slice(block));
        } else {
            // HEADER2_BYTES + bits
            stats.sparse_blocks += 1;
            for p in set_bits(block) {
                stats.data.push(p);
            }
        }
    }

    assert_eq!(remainder.len(), 0, "bug: chunk must have 1<<16 bits");
    stats
}

pub(crate) fn set_bits(block: &[u64]) -> impl Iterator<Item = u8> {
    block.iter().copied().enumerate().flat_map(|(k, mut b)| {
        iter::from_fn(move || {
            if b > 0 {
                let pos = (k as u32) * u64::BITS + b.trailing_zeros();
                b ^= b.lsb();
                Some(pos as u8)
            } else {
                None
            }
        })
    })
}
