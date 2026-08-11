load("//bazel:rust.bzl", "rust")

rust.library(
    name = "func",
    srcs = [
        "src/function.rs",
        "src/lib.rs",
    ],
    proc_macro_deps = ["//syx/cas_cbor2"],
    visibility = ["//visibility:public"],
    deps = [
        "//syx/cas",
        "@crates//:cbor2",
        "@crates//:serde",
    ],
)

rust.test_suite(
    name = "func_test_suite",
    size = "small",
    srcs = [
        "tests/command_digest_is_deterministic.rs",
        "tests/command_digest_is_sensitive.rs",
        "tests/function_digest_is_deterministic.rs",
        "tests/function_digest_is_sensitive.rs",
        "tests/function_resolves_from_store.rs",
    ],
    data = ["//:.rustfmt.toml"],
    shared_srcs = ["tests/common/mod.rs"],
    deps = [
        ":func",
        "//syx/cas",
        "//syx/cas_testing",
        "//syx/ply",
        "//syx/testing",
        "@crates//:object_store",
        "@crates//:tokio",
    ],
)
