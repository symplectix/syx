//! Helpers to implement tests.

use std::fs::File;
use std::path::{
    Path,
    PathBuf,
};
use std::sync::LazyLock as Lazy;
use std::{
    env,
    fs,
    io,
};

use rand::RngExt as _;
use runfiles::Runfiles;
pub use tempfile::TempDir;

pub mod s3;

/// Absolute path to a private writable directory.
pub static TMPDIR: Lazy<PathBuf> = Lazy::new(|| {
    env::var("TEST_TMPDIR").map(PathBuf::from).unwrap_or_else(|_var_err| env::temp_dir())
});

static RUNFILES: Lazy<Runfiles> =
    Lazy::new(|| Runfiles::create().expect("failed to create Runfiles"));

/// Returns the runtime path of a runfile, e.g. `rlocation("_main/path/to/file")`.
pub fn rlocation(path: impl AsRef<Path>) -> PathBuf {
    runfiles::rlocation!(RUNFILES, path.as_ref()).expect("failed to resolve the runfile")
}

/// Create a new temporary directory in [`TMPDIR`].
/// The directory is automatically removed when the `TempDir` [drop](std::ops::Drop)s.
pub fn tempdir() -> TempDir {
    tempfile::tempdir_in(&*TMPDIR).expect("creating a temporary directory in testing::TMPDIR")
}

/// Creates a new temporary directory in the `path` adjoined to [`TMPDIR`].
/// Panics if the `path` is not relative.
pub fn tempdir_in<P: AsRef<Path>>(path: P) -> TempDir {
    assert!(path.as_ref().is_relative());
    let dir = Path::new(&*TMPDIR).join(path);
    fs::create_dir_all(&dir)
        .and_then(|_| tempfile::tempdir_in(&dir))
        .expect("creating a temporary directory in testing::TMPDIR")
}

/// `len` random bytes.
pub fn random_bytes(len: usize) -> Vec<u8> {
    let mut out = vec![0u8; len];
    rand::rng().fill(&mut out[..]);
    out
}

mod private {
    pub trait Sealed {}
    impl Sealed for tempfile::TempDir {}
}

/// Extension trait for TempDir.
pub trait TempDirExt: private::Sealed {
    /// Creates a new temporary file in `self.path()`.
    ///
    /// For various reasons, getting a `Path` from a `File` is not trivial.
    /// If you need a temporary file and its path,
    /// [`create_file`](TempDirExt::create_file) is available for such case.
    fn tempfile(&self) -> File;

    /// Creates a new temporary file at the `path` adjoined to `self.path()`.
    /// Panics if the `path` is not relative.
    ///
    /// Note that reopening a file with the same path does not necessarily open the same file.
    fn create_file<P>(&self, options: &fs::OpenOptions, path: P) -> io::Result<(File, PathBuf)>
    where
        P: AsRef<Path>;
}

impl TempDirExt for TempDir {
    fn tempfile(&self) -> File {
        tempfile::tempfile_in(self.path()).expect("creating a temporary file")
    }

    fn create_file<P>(&self, options: &fs::OpenOptions, path: P) -> io::Result<(File, PathBuf)>
    where
        P: AsRef<Path>,
    {
        assert!(path.as_ref().is_relative());

        let filepath = self.path().join(path);
        let Some(dir) = filepath.parent() else {
            return Err(io::Error::other(format!("no parent '{}'", filepath.display())));
        };

        fs::create_dir_all(dir).and_then(|_| options.open(&filepath)).map(|file| (file, filepath))
    }
}
