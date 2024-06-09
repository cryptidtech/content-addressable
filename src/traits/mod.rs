// SPDX-License-Identifier: Apache-2.0

/// Abstract block storage interface
pub mod blocks;
pub use blocks::Blocks;

/// Abstract mapping of ID to Cid
pub mod cid_map;
pub use cid_map::CidMap;
