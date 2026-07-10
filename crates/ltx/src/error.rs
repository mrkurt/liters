use crate::{Checksum, Pos};

/// Errors produced by the LTX codec.
///
/// Validation errors carry messages mirroring the Go implementation so oracle
/// tests can compare failure modes.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Magic bytes did not match `"LTX1"`. (ltx.go:45)
    #[error("invalid LTX file")]
    InvalidFile,

    /// The trailer's file checksum did not match the computed checksum. (ltx.go:51)
    #[error("file checksum mismatch")]
    ChecksumMismatch,

    /// A snapshot's computed post-apply checksum did not match the trailer.
    #[error("post-apply checksum in trailer ({trailer}) does not match calculated checksum ({computed})")]
    PostApplyChecksumMismatch { trailer: Checksum, computed: Checksum },

    /// An LTX file is not contiguous with the current database position. (ltx.go:112)
    #[error("ltx position mismatch ({0})")]
    PosMismatch(Pos),

    /// Header, trailer, page-header, or state-machine validation failure.
    /// The message mirrors the corresponding Go error string.
    #[error("{0}")]
    Invalid(String),

    /// Malformed LZ4 frame in a page block.
    #[error("lz4: {0}")]
    Lz4(String),

    #[error(transparent)]
    Io(#[from] std::io::Error),
}

impl Error {
    pub(crate) fn invalid(msg: impl Into<String>) -> Self {
        Error::Invalid(msg.into())
    }
}
