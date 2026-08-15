use std::io;

use bitflags::bitflags;
use bytes::{
    BufMut,
    Bytes,
    BytesMut,
};
use content_addressing::Digest;
use slatedb::{
    MergeOperator,
    MergeOperatorError,
};

use super::Entry;

bitflags! {
    /// The trailing byte of `slatedb` value. Never seen by anything
    /// above `Entry`: it says whether that value *is* the content
    /// or a pointer to where the content currently lives instead.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    struct EntryFlags: u8 {
        /// This entry isn't content, it's a pointer to where the
        /// content lives instead.
        const PACKED = 1 << 0;
    }
}

impl Entry {
    /// The tag byte trails the payload rather than leading it, so the
    /// common `Inline` case can append in place via `try_into_mut`,
    /// reusing `bytes`' own allocation, instead of copying a chunk's
    /// entire content, which can run to a few MiB, just to prepend one
    /// byte.
    pub(super) fn encode(self) -> Bytes {
        match self {
            Entry::Inline(bytes) => {
                let mut buf =
                    bytes.try_into_mut().unwrap_or_else(|bytes| BytesMut::from(&bytes[..]));
                buf.put_u8(EntryFlags::empty().bits());
                buf.freeze()
            }
            Entry::Packed { pack_id, offset, length } => {
                let mut buf = BytesMut::with_capacity(32 + 8 + 8 + 1);
                buf.extend_from_slice(pack_id.as_ref());
                buf.extend_from_slice(&offset.to_be_bytes());
                buf.extend_from_slice(&length.to_be_bytes());
                buf.put_u8(EntryFlags::PACKED.bits());
                buf.freeze()
            }
        }
    }

    pub(super) fn decode(bytes: &Bytes) -> io::Result<Self> {
        let Some(&tag) = bytes.last() else {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "malformed Entry"));
        };
        let tag = EntryFlags::from_bits_retain(tag);
        if !tag.contains(EntryFlags::PACKED) {
            return Ok(Entry::Inline(bytes.slice(..bytes.len() - 1)));
        }
        if bytes.len() != 32 + 8 + 8 + 1 {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "malformed Entry"));
        }
        let mut pack_id = [0u8; 32];
        pack_id.copy_from_slice(&bytes[0..32]);
        let offset = u64::from_be_bytes(bytes[32..40].try_into().unwrap());
        let length = u64::from_be_bytes(bytes[40..48].try_into().unwrap());
        Ok(Entry::Packed { pack_id: Digest::new(pack_id), offset, length })
    }
}

/// The merge operator for `pending_bytes`/`pending_keys`.
struct PendingMergeOperator;

impl MergeOperator for PendingMergeOperator {
    fn merge(
        &self,
        key: &Bytes,
        existing: Option<Bytes>,
        op: Bytes,
    ) -> Result<Bytes, MergeOperatorError> {
        if key.ends_with(b"pending_bytes") {
            let existing = existing
                .map(|v| u64::from_be_bytes(v.as_ref().try_into().unwrap_or_default()))
                .unwrap_or(0);
            let delta = u64::from_be_bytes(op.as_ref().try_into().unwrap_or_default());
            return Ok(Bytes::copy_from_slice(&existing.saturating_add(delta).to_be_bytes()));
        }

        match existing {
            Some(existing) => {
                // Grow `existing` in place when uniquely owned.
                let mut buf = existing.try_into_mut().unwrap_or_else(|b| BytesMut::from(&b[..]));
                buf.extend_from_slice(&op);
                Ok(buf.freeze())
            }
            None => Ok(op),
        }
    }
}

/// The merge operator `db` must be opened with for `pending_bytes` and
/// `pending_keys` to work.
pub(super) fn merge_operator() -> Box<dyn MergeOperator + Send + Sync> {
    Box::new(PendingMergeOperator)
}
