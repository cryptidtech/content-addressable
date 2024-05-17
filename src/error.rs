// SPDX-License-Idnetifier: Apache-2.0

/// Errors created by this library
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// formatting error
    #[error(transparent)]
    Fmt(#[from] std::fmt::Error),
    /// I/O error
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// Persist error
    #[error(transparent)]
    Persist(#[from] tempfile::PersistError),

    /// A multicid error
    #[error(transparent)]
    Multicid(#[from] multicid::Error),
    /// A multicodec error
    #[error(transparent)]
    Multicodec(#[from] multicodec::Error),
    /// A multihash error
    #[error(transparent)]
    Multihash(#[from] multihash::Error),
    /// A multitrait error
    #[error(transparent)]
    Multitrait(#[from] multitrait::Error),
    /// A multiutil error
    #[error(transparent)]
    Multiutil(#[from] multiutil::Error),
    /// An FsBlocks error
    #[error(transparent)]
    FsBlocks(#[from] FsBlocksError),

    /// A custom error for callback functions
    #[error("Custom error: {0}")]
    Custom(String),
    /// A wraps any error
    #[error(transparent)]
    Wrapped(#[from] Box<dyn std::error::Error>)
}

/// Error from FsBlocks
#[derive(Clone, Debug, thiserror::Error)]
#[non_exhaustive]
pub enum FsBlocksError {
    /// unsupported base encoding for Cids
    #[error("Unsupported base encoding {0:?}")]
    UnsupportedBaseEncoding(multibase::Base),
    /// the path exists but it isn't a dir
    #[error("Path is not a directory {0}")]
    NotDir(std::path::PathBuf),
    /// the cid for a block is invalid
    #[error("Invalid cid for block {0}")]
    InvalidCid(multicid::EncodedCid),
    /// the cid doesn't refer to a block
    #[error("No such block {0}")]
    NoSuchBlock(multicid::EncodedCid),
}
