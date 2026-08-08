//! Spawns a local S3-compatible server, for tests that need a real remote
//! rather than `object_store::memory::InMemory` or
//! `object_store::local::LocalFileSystem`. Backed by MinIO, an
//! implementation detail callers shouldn't need to care about.
//!
//! Linux/x86_64 only -- the `minio_static` runfile is a static Linux/amd64
//! binary. Callers are expected to gate their own test on the same platform
//! (`target_compatible_with` in `BUILD`, `#[cfg(...)]` in the test itself).

use std::process::Stdio;
use std::{
    io,
    process,
};

use crate::TempDir;

/// Local test credentials. Not a secret.
pub const ACCESS_KEY_ID: &str = "minioadmin";

/// See [`ACCESS_KEY_ID`].
pub const SECRET_ACCESS_KEY: &str = "minioadmin";

/// An S3-compatible server running on localhost, killed when dropped.
pub struct Server {
    child:    process::Child,
    _data:    TempDir,
    endpoint: String,
}

impl Server {
    /// Spawn a server bound to a free localhost port, rooted at `data`.
    /// Buckets aren't created by this -- callers create them through the
    /// S3 API once the server is up.
    ///
    /// Doesn't block for readiness -- relies on the S3 client's own retry
    /// behavior (`object_store`'s AWS client retries on connection failure,
    /// not just HTTP-level errors) to ride out the short window before this
    /// server is actually accepting connections.
    pub fn spawn(data: TempDir) -> io::Result<Self> {
        let port = free_port()?;
        let bin = crate::rlocation("minio_static/file/downloaded");

        let child = process::Command::new(bin)
            .arg("server")
            .arg(data.path())
            .arg("--address")
            .arg(format!("127.0.0.1:{port}"))
            .arg("--console-address")
            .arg("127.0.0.1:0")
            .env("MINIO_ROOT_USER", ACCESS_KEY_ID)
            .env("MINIO_ROOT_PASSWORD", SECRET_ACCESS_KEY)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?;

        Ok(Server { child, _data: data, endpoint: format!("http://127.0.0.1:{port}") })
    }

    /// This server's HTTP endpoint (`http://127.0.0.1:<port>`).
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn free_port() -> io::Result<u16> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    listener.local_addr().map(|addr| addr.port())
}
