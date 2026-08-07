//! `cas::blob::{Exists, Get, Put}` backends built on `object_store`.

mod adapter;

pub use adapter::Adapter;
