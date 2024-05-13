// SPDX-License-Identifier: Apache-2.0

use multicid::Cid;

/// Abstract block storage trait for getting and putting content addressed data
pub trait Blocks {
    /// The error type returned
    type Error;

    /// Try to get a block from its content address 
    fn get(&self, cid: &Cid) -> Result<Vec<u8>, Self::Error>;

    /// Try to put a block into storage. Calls back usin get_cid to generate the Cid for the data
    fn put<D, F>(&mut self, data: &D, get_cid: F) -> Result<Cid, Self::Error>
    where
        D: AsRef<[u8]>,
        F: Fn(&D) -> Result<Cid, Self::Error>;
}