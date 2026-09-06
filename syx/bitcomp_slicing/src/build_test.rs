use bitcomp_core::{
    Bits as _,
    BitsMut as _,
    Block as _,
    Page,
};

use crate::BitVec;
use crate::build::{
    set_bits,
    sparse_chunk_stats,
};
use crate::chunk::Chunk;
use crate::consts::{
    BLOCK_U64_LEN,
    DENSE_CHUNK_BYTES,
};

fn setwords(bits: &mut [u64], x: u64) {
    for b in bits {
        *b = x;
    }
}

#[derive(Debug, Clone)]
enum ChunkBitPattern {
    FullOfSetBits,
    DenseClearly,
    DenseByInspecting,
    SparseSetTwoBits,
    SparseWithSparseBlocksOnly,
    SparseWithTwoTypesOfBlocks,
}

impl ChunkBitPattern {
    fn prep(&self, bits: &mut Page<[u64; 1024]>) {
        use ChunkBitPattern::*;
        let bits = bits.or_empty();
        match self {
            FullOfSetBits => {
                setwords(bits, 0x_ffff_ffff_ffff_ffff); // 11111111...
                assert_eq!(bits.count1(), 8 * 8 * 1024);
            }
            DenseClearly => {
                setwords(bits, 0x_5555_5555_5555_5555); // 01010101...
                assert_eq!(bits.count1(), 4 * 8 * 1024);
            }
            DenseByInspecting => {
                setwords(bits, 0x_1111_1111_1111_1111); // 00010001...
                assert_eq!(bits.count1(), 2 * 8 * 1024);

                let stats = sparse_chunk_stats(bits);
                assert!(stats.bytes() >= DENSE_CHUNK_BYTES);
                assert_eq!(stats.dense_blocks, 256);
            }
            SparseSetTwoBits => {
                bits.set1(0);
                bits.set1(65535);

                let stats = sparse_chunk_stats(bits);
                assert_eq!(stats.bytes(), 2 * 2 + 2);
                assert_eq!(stats.bits(), 2);
                assert_eq!(stats.sparse_blocks, 2);
            }
            SparseWithSparseBlocksOnly => {
                setwords(bits, 1);
                assert_eq!(bits.count1(), 1024);

                let stats = sparse_chunk_stats(bits);
                assert!(stats.bytes() < DENSE_CHUNK_BYTES);
                assert_eq!(stats.bytes(), 2 * 256 + 1024);
                assert_eq!(stats.sparse_blocks, 256);
            }
            SparseWithTwoTypesOfBlocks => {
                for (i, b) in bits.as_chunks_mut::<BLOCK_U64_LEN>().0.iter_mut().enumerate() {
                    match i {
                        0 => setwords(b, 1),
                        1 => b.set1(0),
                        2 => b.set1(u8::MAX as u64),
                        3 => setwords(b, 0x_5555_5555_5555_5555),
                        4 => setwords(b, 0x_1111_1111_1111_1111),
                        _ => {}
                    }
                }

                let stats = sparse_chunk_stats(bits);
                assert_eq!(stats.bits(), 198);
                // header: 2 * 5
                // two dense blocks: 32 * 2
                // three sparse blocks: 4 + 1 + 1
                assert_eq!(stats.bytes(), 2 * 5 + 64 + 6);
                assert_eq!(stats.dense_blocks, 2);
                assert_eq!(stats.sparse_blocks, 3);
            }
        }
    }

    fn test(&self, i: usize, chunk: Chunk<'_>) {
        use Chunk::*;
        use ChunkBitPattern::*;
        let i = i as u32;
        match (self, chunk) {
            (FullOfSetBits, Full { index }) => {
                assert_eq!(i, index, "index not match");
            }
            (DenseClearly, Dense { index, bits, data }) => {
                assert_eq!(i, index, "index not match");
                assert_eq!(bits, 4 * 8 * 1024);
                let mut bits = Page::<[u64; 1024]>::empty();
                self.prep(&mut bits);
                assert_eq!(bytemuck::cast_slice::<u64, u8>(bits.as_ref().unwrap()), data);
            }
            (DenseByInspecting, Dense { index, bits, data }) => {
                assert_eq!(i, index, "index not match");
                assert_eq!(bits, 2 * 8 * 1024);
                let mut bits = Page::<[u64; 1024]>::empty();
                self.prep(&mut bits);
                assert_eq!(bytemuck::cast_slice::<u64, u8>(bits.as_ref().unwrap()), data);
            }
            (SparseSetTwoBits, Sparse { index, bits, mut blocks }) => {
                assert_eq!(i, index, "index not match");
                assert_eq!(bits, 2);
                assert_eq!(blocks.header.len(), 2);

                let b = blocks.next().unwrap();
                assert!(b.is_sparse());
                assert_eq!(b.index, 0);
                assert_eq!(b.bits, 1);
                assert_eq!(b.data(), &[0]);

                let b = blocks.next().unwrap();
                assert!(b.is_sparse());
                assert_eq!(b.index, 255);
                assert_eq!(b.bits, 1);
                assert_eq!(b.data(), &[255]);
            }
            (SparseWithSparseBlocksOnly, Sparse { index, mut bits, blocks }) => {
                assert_eq!(i, index, "index not match");
                assert_eq!(bits, 1024);
                assert_eq!(blocks.header.len(), 256);

                for b in blocks {
                    assert!(b.is_sparse());
                    assert_eq!(b.data().len(), 4);
                    bits -= b.bits as u32;
                }
                assert_eq!(bits, 0);
            }
            (SparseWithTwoTypesOfBlocks, Sparse { index, mut bits, blocks }) => {
                assert_eq!(i, index, "index not match");
                assert_eq!(bits, 198);
                assert_eq!(blocks.header.len(), 5);

                for b in blocks {
                    bits -= b.bits as u32;
                }
                assert_eq!(bits, 0);
            }
            (pat, chunk) => {
                panic!("the chunk does not match to the pattern ({pat:?}): {chunk:?}");
            }
        }
    }
}

#[test]
fn encode_decode_chunk_bit_patterns_as_expected() {
    use ChunkBitPattern::*;
    let indexed_pats = vec![
        (1 << 0, FullOfSetBits),
        (1 << 2, DenseClearly),
        (1 << 4, SparseSetTwoBits),
        (1 << 6, DenseByInspecting),
        (1 << 8, SparseWithSparseBlocksOnly),
        (1 << 10, SparseWithTwoTypesOfBlocks),
        (1 << 12, SparseSetTwoBits),
        (1 << 14, FullOfSetBits),
        ((1 << 16) - 1, SparseSetTwoBits),
    ];

    let bv = {
        let mut vec = vec![Page::<[u64; 1024]>::empty(); 1 << 16];
        for (i, pat) in &indexed_pats {
            pat.prep(&mut vec[*i]);
        }
        BitVec::build(vec.as_slice())
    };

    let mut chunks = bv.chunks();
    assert_eq!(chunks.len(), indexed_pats.len());

    for (c, (i, pat)) in chunks.by_ref().zip(&indexed_pats) {
        pat.test(*i, c);
    }
}

#[test]
fn iterate_over_set_bits() {
    let mut bv = vec![0u64; 4];
    bv.set1(0);
    bv.set1(1);
    bv.set1(63);
    bv.set1(64);
    bv.set1(255);

    let mut bits = set_bits(&bv);
    assert_eq!(bits.next().unwrap(), 0);
    assert_eq!(bits.next().unwrap(), 1);
    assert_eq!(bits.next().unwrap(), 63);
    assert_eq!(bits.next().unwrap(), 64);
    assert_eq!(bits.next().unwrap(), 255);
    assert_eq!(bits.next(), None);
}
