//! Function: a content-addressed reference to something runnable, in one
//! of two calling conventions.

use std::collections::BTreeMap;

/// A program, its arguments, and the environment variables to invoke it
/// with. Shared by:
/// - `Function::Action` (run once)
/// - `Function::Server` (kept warm across calls, invoked repeatedly)
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    cas_cbor2::ToBytes,
    cas_cbor2::FromBytes,
)]
pub struct Command {
    program: String,
    args:    Vec<String>,
    env:     BTreeMap<String, String>,
}

impl Command {
    /// A `Command` running `program` with `args` and `env`.
    pub fn new(program: impl Into<String>) -> Self {
        Command { program: program.into(), args: Vec::new(), env: BTreeMap::new() }
    }

    /// Append one argument.
    pub fn arg<S>(mut self, arg: S) -> Self
    where
        S: AsRef<str>,
    {
        self.args.push(arg.as_ref().to_owned());
        self
    }

    /// Append each argument, in order.
    pub fn args<I, S>(mut self, args: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.args.extend(args.into_iter().map(|a| a.as_ref().to_owned()));
        self
    }

    /// Set one environment variable.
    pub fn env<K, V>(mut self, key: K, value: V) -> Self
    where
        K: AsRef<str>,
        V: AsRef<str>,
    {
        self.env.insert(key.as_ref().to_owned(), value.as_ref().to_owned());
        self
    }

    /// Set each environment variable, in order.
    pub fn envs<I, K, V>(mut self, vars: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<str>,
        V: AsRef<str>,
    {
        self.env
            .extend(vars.into_iter().map(|(k, v)| (k.as_ref().to_owned(), v.as_ref().to_owned())));
        self
    }
}

/// A reference to something runnable, addressed by how it's invoked:
///
/// - `Action`: run once, directly, against input supplied at call time. Cached at the granularity
///   of the whole input: "has this exact input been processed before?".
/// - `Server`: independent, per-blob calls to a server process. Cached per blob, not per call: "has
///   this specific item been processed before?".
// TODO: add a field for which OCI image the VM boots from. The image is
// used directly as the VM's boot rootfs; `config` is then materialized inside
// the booted VM, on top of it.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    cas_cbor2::ToBytes,
    cas_cbor2::FromBytes,
)]
pub enum Function {
    /// Run once, directly.
    Action {
        /// The program to run.
        command: cas::Digest,
        /// Its configuration, a `Tree`, materialized before `command` runs.
        config:  cas::Digest,
    },
    /// Call a process kept warm across independent, per-blob requests.
    Server {
        /// The process to call, started on demand and shut down when idle.
        command: cas::Digest,
        /// Its configuration, a `Tree`.
        config:  cas::Digest,
    },
}

impl Function {
    /// Run `command` once, directly, configured by `config` (a `Tree`),
    /// against input supplied at call time.
    pub fn action(command: cas::Digest, config: cas::Digest) -> Self {
        Function::Action { command, config }
    }

    /// Run `server`, kept warm across calls, configured by `config` (a
    /// `Tree`), called with independent per-item requests.
    pub fn server(command: cas::Digest, config: cas::Digest) -> Self {
        Function::Server { command, config }
    }
}
