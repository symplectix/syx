//! `Function`'s digest is deterministic.

#[test]
fn hashing_the_same_action_function_twice_gives_the_same_digest() {
    let command = cas_testing::digest_bytes(b"command");
    let config = cas_testing::digest_bytes(b"config");
    let a = func::Function::action(command, config);
    let b = func::Function::action(command, config);
    assert_eq!(cas_testing::digest(&a), cas_testing::digest(&b));
}

#[test]
fn hashing_the_same_server_function_twice_gives_the_same_digest() {
    let command = cas_testing::digest_bytes(b"command");
    let config = cas_testing::digest_bytes(b"config");
    let a = func::Function::server(command, config);
    let b = func::Function::server(command, config);
    assert_eq!(cas_testing::digest(&a), cas_testing::digest(&b));
}
