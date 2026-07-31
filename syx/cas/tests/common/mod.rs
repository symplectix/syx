//! Shared fixtures for `cas`'s external test suite.
// `shared_srcs` compiles this file into every test binary in the
// suite separately, so not every item here is used by every one.
#![allow(dead_code)]

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct Example {
    pub name:  String,
    pub count: u32,
}

impl cas::ToBytes for Example {
    type Error = cbor2::ser::Error;

    fn to_bytes(&self) -> Result<cas::Bytes, Self::Error> {
        cbor2::to_canonical_vec(self).map(cas::Bytes::from)
    }
}

impl cas::FromBytes for Example {
    type Error = cbor2::de::Error;

    fn from_bytes(bytes: cas::Bytes) -> Result<Self, Self::Error> {
        cbor2::from_slice(&bytes)
    }
}
