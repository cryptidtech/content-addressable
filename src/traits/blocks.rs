// SPDX-License-Identifier: Apache-2.0
use multicid::Cid;

/// Abstract block storage trait for getting and putting content addressed data
pub trait Blocks {
    /// The error type returned
    type Error;

    /// Try to confirm a block exists
    fn exists(&self, cid: &Cid) -> Result<bool, Self::Error>;

    /// Try to get a block from its content address 
    fn get(&self, cid: &Cid) -> Result<Vec<u8>, Self::Error>;

    /// Try to put a block into storage. This calls the get_cid closure to calculate the Cid over
    /// the data passed in. It also calls the pre_commit closure after the put transaction has been
    /// set up successfully but before it is committed. This allows for other side effects to
    /// happen before the block is committed. If the pre_commit closure returns an Err, the put is
    /// not committed and the Err is propagated back to the caller of put.
    fn put<D, F1, F2>(&mut self, data: &D, get_cid: F1, pre_commit: F2) -> Result<Cid, Self::Error>
    where
        D: AsRef<[u8]>,
        F1: Fn(&D) -> Result<Cid, Self::Error>,
        F2: Fn(&Cid) -> Result<(), Self::Error>;

    /// Try to remove a block from storage
    fn rm(&self, cit: &Cid) -> Result<Vec<u8>, Self::Error>;
}
