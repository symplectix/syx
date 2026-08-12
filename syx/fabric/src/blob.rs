//! Tree: a content-addressed collection of blobs.

use std::collections::{
    BTreeMap,
    BTreeSet,
};

/// What a `Tree` entry's name points to: a file's content, or a nested
/// `Tree`, each referenced by digest rather than embedded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum Node {
    /// A file's content.
    Blob(cas::Digest),
    /// A nested `Tree`.
    Tree(cas::Digest),
}

/// A content-addressed tree of blobs: names mapped to a `Blob` or a
/// nested `Tree`. Entries are sorted by name and a name can't appear
/// twice, meaning the same entries built in any order produce the same
/// `Tree`. Two entries may still point at the same underlying digest
/// (e.g. two identically-named-differently files with the same content).
/// Uniqueness is on the name, not the value, so `Tree` can represent a
/// multiset of items as long as each has a distinct name, which is the
/// normal case (files always have distinct paths).
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    cas_cbor2::ToBytes,
    cas_cbor2::FromBytes,
)]
pub struct Tree {
    entries: BTreeMap<String, Node>,
    /// Additional blobs a producer created while building this tree but
    /// that aren't reachable from any entry's name.
    /// Recording them here keeps them from looking unreferenced to a GC
    /// walking the CAS from live roots -- `entries` alone can't express
    /// "reachable but not a real file", since every entry is materialized.
    /// A digest is either reachable via this tree or not, so this is a
    /// set: interning the same digest twice doesn't change the tree.
    interns: BTreeSet<cas::Digest>,
}

impl Tree {
    /// Build a `Tree` from `entries` and `interns`.
    pub fn new(
        entries: impl IntoIterator<Item = (String, Node)>,
        interns: impl IntoIterator<Item = cas::Digest>,
    ) -> Self {
        Tree { entries: entries.into_iter().collect(), interns: interns.into_iter().collect() }
    }
}
