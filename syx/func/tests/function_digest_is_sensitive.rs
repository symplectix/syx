//! `Function`'s digest is sensitive to its input.

#[test]
fn different_command_produces_different_action_function_digests() {
    let config = cas_testing::digest_bytes(b"config");
    let a = func::Function::action(cas_testing::digest_bytes(b"command-a"), config);
    let b = func::Function::action(cas_testing::digest_bytes(b"command-b"), config);
    assert_ne!(cas_testing::digest(&a), cas_testing::digest(&b));
}

#[test]
fn different_config_produces_different_action_function_digests() {
    let command = cas_testing::digest_bytes(b"command");
    let a = func::Function::action(command, cas_testing::digest_bytes(b"config-a"));
    let b = func::Function::action(command, cas_testing::digest_bytes(b"config-b"));
    assert_ne!(cas_testing::digest(&a), cas_testing::digest(&b));
}

#[test]
fn different_command_produces_different_server_function_digests() {
    let config = cas_testing::digest_bytes(b"config");
    let a = func::Function::server(cas_testing::digest_bytes(b"command-a"), config);
    let b = func::Function::server(cas_testing::digest_bytes(b"command-b"), config);
    assert_ne!(cas_testing::digest(&a), cas_testing::digest(&b));
}

#[test]
fn different_config_produces_different_server_function_digests() {
    let command = cas_testing::digest_bytes(b"command");
    let a = func::Function::server(command, cas_testing::digest_bytes(b"config-a"));
    let b = func::Function::server(command, cas_testing::digest_bytes(b"config-b"));
    assert_ne!(cas_testing::digest(&a), cas_testing::digest(&b));
}

#[test]
fn action_and_server_variants_do_not_collide_on_the_same_command_and_config() {
    let command = cas_testing::digest_bytes(b"command");
    let config = cas_testing::digest_bytes(b"config");
    let a = func::Function::action(command, config);
    let b = func::Function::server(command, config);
    assert_ne!(cas_testing::digest(&a), cas_testing::digest(&b));
}
