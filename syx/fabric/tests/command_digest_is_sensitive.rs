//! `Command`'s digest is sensitive to its input.

mod common;
use common::command;

#[test]
fn different_program_produces_different_command_digests() {
    let a = command("echo", &["hi"]);
    let b = command("cat", &["hi"]);
    assert_ne!(cas_testing::digest(&a), cas_testing::digest(&b));
}

#[test]
fn different_args_produce_different_command_digests() {
    let a = command("echo", &["a"]);
    let b = command("echo", &["b"]);
    assert_ne!(cas_testing::digest(&a), cas_testing::digest(&b));
}

#[test]
fn different_env_produces_different_command_digests() {
    let a = fabric::Command::new("run").env("KEY", "a");
    let b = fabric::Command::new("run").env("KEY", "b");
    assert_ne!(cas_testing::digest(&a), cas_testing::digest(&b));
}
