//! fabric: content-addressed trees and references to runnable things.

mod blob;
mod function;
mod graph;

pub use blob::{
    Node,
    Tree,
};
pub use function::{
    Command,
    Function,
};
pub use graph::Graph;
