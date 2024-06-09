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
    /// A multikey error
    #[error(transparent)]
    Multikey(#[from] multikey::Error),
    /// A multitrait error
    #[error(transparent)]
    Multitrait(#[from] multitrait::Error),
    /// A multiutil error
    #[error(transparent)]
    Multiutil(#[from] multiutil::Error),
    /// An FsStorage
    #[error(transparent)]
    FsStorage(#[from] FsStorageError),

    /// A custom error for callback functions
    #[error("Custom error: {0}")]
    Custom(String),
    /// A wraps any error
    #[error(transparent)]
    Wrapped(#[from] Box<dyn std::error::Error>)
}

/// Error from FsStorage
#[derive(Clone, Debug, thiserror::Error)]
#[non_exhaustive]
pub enum FsStorageError {
    /// unsupported base encoding for Cids
    #[error("Unsupported base encoding {0:?}")]
    UnsupportedBaseEncoding(multibase::Base),
    /// the path exists but it isn't a dir
    #[error("Path is not a directory {0}")]
    NotDir(std::path::PathBuf),
    /// the id for the data is invalid
    #[error("Invalid id {0}")]
    InvalidId(String),
    /// the id doesn't refer to data
    #[error("No such data {0}")]
    NoSuchData(String),
}
