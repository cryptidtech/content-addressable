// SPDX-License-Identifier: Apache-2.0

//! content-addressable
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
pub use impls::prelude::*;

/// Traits from this crate
pub mod traits;
pub use traits::{blocks::Blocks, cid_map::CidMap};

/// Prelude convenience
pub mod prelude {
    pub use super::*;
    /// re-exports
    pub use multicid::{Cid, Vlad};
    pub use multicodec::Codec;
    pub use multikey::Multikey;
    pub use multiutil::BaseEncoded;
}
