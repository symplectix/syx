//! `Command`'s digest is deterministic.

mod common;
use common::command;

#[test]
fn hashing_the_same_command_twice_gives_the_same_digest() {
    let a = command("echo", &["hi"]);
    let b = command("echo", &["hi"]);
    assert_eq!(cas_testing::digest(&a), cas_testing::digest(&b));
}
