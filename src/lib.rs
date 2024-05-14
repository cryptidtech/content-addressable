// SPDX-License-Identifier: Apache-2.0

//!
#![warn(missing_docs)]
#![deny(
    trivial_casts,
    trivial_numeric_casts,
    unused_import_braces,
    unused_qualifications,
)]

/// Errors produced by this library
pub mod error;
pub use error::Error;

/// Implementations of the traits
pub mod impls;
pub use impls::fsblocks::{self, FsBlocks};

/// Traits from this crate
pub mod traits;
pub use traits::blocks::Blocks;

/// Serde serialization
#[cfg(features = "serde")]
pub mod serde;

/// Prelude convenience
pub mod prelude {
    pub use super::*;
    /// re-exports
    pub use multicid::Cid;
    pub use multicodec::Codec;
    pub use multiutil::BaseEncoded;
}
