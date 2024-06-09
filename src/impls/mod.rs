// SPDX-License-Identifier: Apache-2.0

/// Filesystem backed block storage
pub mod fsblocks;
pub use fsblocks::FsBlocks;

/// Filesystem backed multikey_map storage
pub mod fsmultikey_map;
pub use fsmultikey_map::FsMultikeyMap;

/// Generic content addressable storage
pub mod fsstorage;
pub use fsstorage::FsStorage;

/// Filesystem backed multikey_map storage
pub mod fsvlad_map;
pub use fsvlad_map::FsVladMap;

/// Simple way to import all public symbols
pub mod prelude {
    pub use super::*;
}
