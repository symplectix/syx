use std::slice;

use crate::consts::*;

#[derive(Debug, Clone)]
pub struct Blocks<'a> {
    pub(crate) header: BlocksHeader<'a>,
    data: &'a [u8],
}

impl<'a> Blocks<'a> {
    pub(crate) fn new(header_len: usize, bytes: &'a [u8]) -> Blocks<'a> {
        let (header, data) = bytes.split_at(header_len);
        Blocks { header: BlocksHeader::new(header), data }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct Block<'a> {
    pub(crate) index: u16,
    pub(crate) bits: u16,
    data: BlockData<'a>,
}

#[cfg(test)]
impl<'a> Block<'a> {
    pub(crate) fn data(&self) -> &'a [u8] {
        match self.data {
            BlockData::Dense(v) => v,
            BlockData::Sparse(v) => v,
        }
    }

    pub(crate) fn is_dense(&self) -> bool {
        matches!(self.data, BlockData::Dense(_))
    }

    pub(crate) fn is_sparse(&self) -> bool {
        matches!(self.data, BlockData::Sparse(_))
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum BlockData<'a> {
    Dense(&'a [u8]),
    Sparse(&'a [u8]),
}

#[derive(Debug, Clone)]
pub(crate) struct BlocksHeader<'a> {
    chunks:    slice::Iter<'a, [u8; BLOCK_INFO_SIZE]>,
    #[cfg_attr(not(test), allow(dead_code))]
    remainder: &'a [u8],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BlockInfo {
    index: u16,
    bits:  u16,
}

impl<'a> Iterator for Blocks<'a> {
    type Item = Block<'a>;
    fn next(&mut self) -> Option<Self::Item> {
        self.header.next().map(|BlockInfo { index, bits }| Block {
            index,
            bits,
            data: if bits >= SPARSE_BLOCK_THRESHOLD as u16 {
                let (data, remain) = self.data.split_at(DENSE_BLOCK_BYTES);
                self.data = remain;
                BlockData::Dense(data)
            } else {
                let (data, remain) = self.data.split_at(bits as usize);
                self.data = remain;
                BlockData::Sparse(data)
            },
        })
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.header.size_hint()
    }
}
impl<'a> ExactSizeIterator for Blocks<'a> {}

impl<'a> BlocksHeader<'a> {
    fn new(header: &'a [u8]) -> Self {
        let (chunks, remainder) = header.as_chunks::<BLOCK_INFO_SIZE>();
        BlocksHeader { chunks: chunks.iter(), remainder }
    }

    #[cfg(test)]
    pub(crate) fn remainder(&self) -> &[u8] {
        self.remainder
    }
}
impl<'a> Iterator for BlocksHeader<'a> {
    type Item = BlockInfo;
    fn next(&mut self) -> Option<Self::Item> {
        self.chunks
            .next()
            .map(|slice| BlockInfo { index: slice[0] as u16, bits: slice[1] as u16 + 1 })
    }
    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.chunks.size_hint()
    }
}
impl<'a> ExactSizeIterator for BlocksHeader<'a> {}
