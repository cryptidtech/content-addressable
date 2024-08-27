// SPDX-License-Identifier: Apache-2.0
use crate::{Blocks, Error, error::FsStorageError, fsstorage::{self, FsStorage}};
use log::debug;
use multibase::Base;
use multicid::Cid;
use std::{fs::{self, File}, io::{Read, Write}, path::{Path, PathBuf}};

/// The FsBlocks type uses CID's
pub type FsBlocks = FsStorage<Cid>;

/// Builder for a FsBlock instance
#[derive(Clone, Debug, Default)]
pub struct Builder {
    root: PathBuf,
    lazy: bool,
    base_encoding: Option<Base>,
}

impl Builder {
    /// create a new builder from the root path, this defaults to lazy
    pub fn new<P: AsRef<Path>>(root: P) -> Self {
        debug!("fsblocks::Builder::new({})", root.as_ref().display());
        Builder {
            root: root.as_ref().to_path_buf(),
            lazy: true,
            base_encoding: None,
        }
    }

    /// set lazy to false
    pub fn not_lazy(mut self) -> Self {
        self.lazy = false;
        self
    }

    /// set the encoding codec to use for CIDs
    pub fn with_base_encoding(mut self, base: Base) -> Self {
        self.base_encoding = Some(base);
        self
    }

    /// build the instance
    pub fn try_build(&self) -> Result<FsBlocks, Error> {
        let base_encoding = self.base_encoding.unwrap_or(Base::Base32Z);

        let mut builder = fsstorage::Builder::<Cid>::new(&self.root).with_base_encoding(base_encoding);
        if !self.lazy {
            builder = builder.not_lazy();
        }

        builder.try_build()
    }
}

impl Blocks for FsBlocks {
    type Error = Error;

    fn exists(&self, cid: &Cid) -> Result<bool, Self::Error> {
        // get the paths
        let (_, _, file, _) = self.get_paths(cid)?;
        Ok(file.try_exists()?)
    }

    fn get(&self, cid: &Cid) -> Result<Vec<u8>, Self::Error> {
        // get the paths
        let (ecid, subfolder, file, _) = self.get_paths(cid)?;

        // check if it exists and is a dir...otherwise create the dir
        if subfolder.try_exists()? {
            if !subfolder.is_dir() {
                return Err(FsStorageError::NotDir(subfolder).into());
            }
        } else {
            return Err(FsStorageError::NoSuchData(ecid.to_string()).into());
        }

        // store the block in the filesystem
        debug!("fsblocks: Getting block from: {}", file.display());
        let mut f = File::open(&file)?;
        let mut data = Vec::default();
        f.read_to_end(&mut data)?;
        Ok(data)
    }

    fn put<D, F1, F2>(&mut self, data: &D, get_cid: F1, pre_commit: F2) -> Result<Cid, Self::Error>
    where
        D: AsRef<[u8]>,
        F1: Fn(&D) -> Result<Cid, Self::Error>,
        F2: Fn(&Cid) -> Result<(), Self::Error>,
    {
        // call the callback for calculating the CID
        let cid = get_cid(data)?;

        // get the paths
        let (ecid, subfolder, file, _) = self.get_paths(&cid)?;

        // check if it exists and is a dir...otherwise create the dir
        if subfolder.try_exists()? {
            if !subfolder.is_dir() {
                return Err(FsStorageError::NotDir(subfolder).into());
            }
        } else {
            fs::create_dir_all(&subfolder)?;
            debug!("fsblocks: Created subfolder at: {}", subfolder.display());
        }

        // store the block in the filesystem
        debug!("fsblocks: Storing block at: {}", file.display());

        // securely create a temporary file. its name begins with "." so that if something goes
        // wrong, the temporary file will be cleaned up by a future GC pass
        let mut temp = tempfile::Builder::new()
            .suffix(&format!(".{}", ecid))
            .tempfile_in(&subfolder)?;

        // write the contents to the file
        temp.write_all(data.as_ref())?;

        // call the pre_commit closure to give the caller a chance to do other side effects
        pre_commit(&cid)?;

        // atomically rename/move it to the correct location
        temp.persist(&file)?;

        Ok(cid)
    }

    fn rm(&self, cid: &Cid) -> Result<Vec<u8>, Self::Error> {
        // first try to get the value
        let v = self.get(cid)?;

        // get the paths
        let (_, subfolder, file, lazy_deleted_file) = self.get_paths(cid)?;

        // remove the file if it exists
        if file.try_exists()? && file.is_file() {
            if self.lazy {
                // rename the file instead of remove it
                fs::rename(&file, &lazy_deleted_file)?;
                debug!("fsblocks: Lazy deleted block at: {} to {}", file.display(), lazy_deleted_file.display());
            } else {
                // not lazy so delete it
                fs::remove_file(&file)?;
                debug!("fsblocks: Removed block at: {}", file.display());
            }
        }

        // remove the subfolder if it is emtpy and we're not lazy
        if subfolder.try_exists()? && subfolder.is_dir() && fs::read_dir(&subfolder)?.count() == 0 && !self.lazy {
            fs::remove_dir(&subfolder)?;
            debug!("fsblocks: Removed subdir at: {}", subfolder.display());
        }

        Ok(v)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use multicid::cid;
    use multicodec::Codec;
    use multihash::mh;

    #[test]
    fn test_builder_lazy() {
        let mut pb = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        pb.push(".fsblocks1");

        let blocks = Builder::new(&pb).try_build().unwrap();
        assert_eq!(blocks.root, pb);
        assert_eq!(blocks.lazy, true);
        assert_eq!(blocks.base_encoding, Base::Base32Z);
        assert!(pb.try_exists().is_ok());
        assert!(pb.is_dir());

        assert!(fs::remove_dir_all(&pb).is_ok());
    }

    #[test]
    fn test_builder_not_lazy() {
        let mut pb = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        pb.push(".fsblocks2");

        let blocks = Builder::new(&pb).not_lazy().try_build().unwrap();
        assert_eq!(blocks.root, pb);
        assert_eq!(blocks.lazy, false);
        assert_eq!(blocks.base_encoding, Base::Base32Z);
        assert!(pb.try_exists().is_ok());
        assert!(pb.is_dir());

        for d in fs::read_dir(&pb).unwrap() {
            assert!(d.is_ok());
            let dir = d.unwrap();
            assert!(dir.file_type().unwrap().is_dir());
        }

        assert!(fs::remove_dir_all(&pb).is_ok());
    }

    fn put(blocks: &mut FsBlocks, v: impl AsRef<[u8]>) -> Cid {
        let cid = blocks.put(&v, |data| -> Result<Cid, Error> {
            let mh = mh::Builder::new_from_bytes(Codec::Blake3, data)?
                .try_build()?;
            let cid = cid::Builder::new(Codec::Cidv1)
                .with_target_codec(Codec::Identity)
                .with_hash(&mh)
                .try_build()?;
            Ok(cid)
        }, |_| Ok(())).unwrap();
        cid
    }

    #[test]
    fn test_put_lazy() {
        let mut pb = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        pb.push(".fsblocks3");

        let mut blocks = Builder::new(&pb).try_build().unwrap();
        
        let v1 = b"for great justice!".to_vec();
        let cid = put(&mut blocks, &v1);
       
        let v2 = blocks.get(&cid).unwrap();
        assert_eq!(v1, v2);

        assert!(fs::remove_dir_all(&pb).is_ok());
    }

    #[test]
    fn test_put_not_lazy() {
        let mut pb = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        pb.push(".fsblocks4");

        let mut blocks = Builder::new(&pb).not_lazy().try_build().unwrap();
        
        let v1 = b"move every zig!".to_vec();
        let cid = put(&mut blocks, &v1);
        
        let v2 = blocks.get(&cid).unwrap();
        assert_eq!(v1, v2);

        assert!(fs::remove_dir_all(&pb).is_ok());
    }

    #[test]
    fn test_rm_lazy() {
        let mut pb = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        pb.push(".fsblocks5");

        let mut blocks = Builder::new(&pb).try_build().unwrap();
        
        let v1 = b"for great justice!".to_vec();
        let cid = put(&mut blocks, &v1);

        // get the paths to the subfolder and file created from the put
        let (_, _, file, lazy_deleted_file) = blocks.get_paths(&cid).unwrap();

        // lazy delete the block
        let v2 = blocks.rm(&cid).unwrap();
        assert_eq!(v1, v2);

        // this is lazy so the lazy deleted file should sill be there
        assert!(lazy_deleted_file.try_exists().unwrap());
        // and the file should not be there
        assert!(!file.try_exists().unwrap());

        assert!(fs::remove_dir_all(&pb).is_ok());
    }

    #[test]
    fn test_rm_not_lazy() {
        let mut pb = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        pb.push(".fsblocks6");

        let mut blocks = Builder::new(&pb).not_lazy().try_build().unwrap();
        
        let v1 = b"move every zig!".to_vec();
        let cid = put(&mut blocks, &v1);

        // get the paths to the subfolder and file created from the put
        let (_, subfolder, file, lazy_deleted_file) = blocks.get_paths(&cid).unwrap();

        // delete the block
        let v2 = blocks.rm(&cid).unwrap();
        assert_eq!(v1, v2);

        // this is not lazy so the lazy deleted file should not be there
        assert!(!lazy_deleted_file.try_exists().unwrap());
        // and the file should not be there either
        assert!(!file.try_exists().unwrap());
        // and since the subfolder is empty it should not be there either
        assert!(!subfolder.try_exists().unwrap());

        assert!(fs::remove_dir_all(&pb).is_ok());
    }

    #[test]
    fn test_gc() {
        let mut pb = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        pb.push(".fsblocks7");

        let mut blocks = Builder::new(&pb).try_build().unwrap();
        
        let v1 = b"for great justice!".to_vec();
        let cid1 = put(&mut blocks, &v1);
        let v2 = b"move every zig!".to_vec();
        let cid2 = put(&mut blocks, &v2);

        let _ = blocks.rm(&cid1).unwrap();
        let _ = blocks.rm(&cid2).unwrap();

        // lazy delete, check that the file is gone, the lazy delete file and folder still exist
        let (_, subfolder1, file1, lazy_deleted_file1) = blocks.get_paths(&cid1).unwrap();
        assert!(lazy_deleted_file1.try_exists().unwrap());
        assert!(!file1.try_exists().unwrap());
        assert!(subfolder1.try_exists().unwrap());

        // lazy delete, check that the file is gone, the lazy delete file and folder still exist
        let (_, subfolder2, file2, lazy_deleted_file2) = blocks.get_paths(&cid2).unwrap();
        assert!(lazy_deleted_file2.try_exists().unwrap());
        assert!(!file2.try_exists().unwrap());
        assert!(subfolder2.try_exists().unwrap());

        // garbage collect
        blocks.gc().unwrap();

        // no files nor folders should exist
        assert!(!lazy_deleted_file1.try_exists().unwrap());
        assert!(!file1.try_exists().unwrap());
        assert!(!subfolder1.try_exists().unwrap());
        assert!(!lazy_deleted_file2.try_exists().unwrap());
        assert!(!file2.try_exists().unwrap());
        assert!(!subfolder2.try_exists().unwrap());

        assert!(fs::remove_dir_all(&pb).is_ok());
    }
}
