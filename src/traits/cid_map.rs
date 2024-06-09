// SPDX-License-Identifier: Apache-2.0
use multicid::Cid;

/// Abstract storage trait for managing Multikey to Cid mappings
pub trait CidMap<ID> {
    /// The error type returned
    type Error;

    /// Try to confirm a pubkey mapping exists
    fn exists(&self, id: &ID) -> Result<bool, Self::Error>;

    /// Try to get the current mapping value
    fn get(&self, id: &ID) -> Result<Cid, Self::Error>;

    /// Try to update the current mappeing from the ID to the Cid. This returns the current
    /// value if there was one. If the mapping is new, Ok(None) is returned.
    fn put(&mut self, id: &ID, cid: &Cid) -> Result<Option<Cid>, Self::Error>;

    /// Try to remove the current mapping
    fn rm(&self, id: &ID) -> Result<Cid, Self::Error>;
}
