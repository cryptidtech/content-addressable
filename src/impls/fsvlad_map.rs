// SPDX-License-Identifier: Apache-2.0
use crate::{CidMap, Error, error::FsStorageError, fsstorage::{self, FsStorage}};
use log::debug;
use multibase::Base;
use multicid::{Cid, Vlad};
use std::{fs::{self, File}, io::{Read, Write}, path::{Path, PathBuf}};

/// The FsMultikeyMap type uses CID's
pub type FsVladMap = FsStorage<Vlad>;

/// Builder for a FsMultikeyMap instance
#[derive(Clone, Debug, Default)]
pub struct Builder {
    root: PathBuf,
    lazy: bool,
    base_encoding: Option<Base>,
}

impl Builder {
    /// create a new builder from the root path, this defaults to lazy
    pub fn new<P: AsRef<Path>>(root: P) -> Self {
        debug!("fsmultikey_map::Builder::new({})", root.as_ref().display());
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

    /// set the encoding codec to use for mks
    pub fn with_base_encoding(mut self, base: Base) -> Self {
        self.base_encoding = Some(base);
        self
    }

    /// build the instance
    pub fn try_build(&self) -> Result<FsVladMap, Error> {
        let base_encoding = self.base_encoding.unwrap_or(Base::Base32Z);

        let mut builder = fsstorage::Builder::<Vlad>::new(&self.root).with_base_encoding(base_encoding);
        if !self.lazy {
            builder = builder.not_lazy();
        }

        builder.try_build()
    }
}

impl CidMap<Vlad> for FsVladMap {
    type Error = Error;

    fn exists(&self, id: &Vlad) -> Result<bool, Self::Error> {
        // get the paths
        let (_, _, file, _) = self.get_paths(id)?;
        Ok(file.try_exists()?)
    }

    fn get(&self, id: &Vlad) -> Result<Cid, Self::Error> {
        // get the paths
        let (eid, subfolder, file, _) = self.get_paths(id)?;

        // check if it exists and is a dir...otherwise create the dir
        if subfolder.try_exists()? {
            if !subfolder.is_dir() {
                return Err(FsStorageError::NotDir(subfolder).into());
            }
        } else {
            return Err(FsStorageError::NoSuchData(eid.to_string()).into());
        }

        // store the mapping in the filesystem
        debug!("fsmultikey_map: Getting Cid from: {}", file.display());
        let mut f = File::open(&file)?;
        let mut data = Vec::default();
        f.read_to_end(&mut data)?;

        // reconstruct the Cid from the data
        let cid = Cid::try_from(data.as_slice())?;
        Ok(cid)
    }

    fn put(&mut self, id: &Vlad, cid: &Cid) -> Result<Option<Cid>, Self::Error> {
        // get the paths
        let (eid, subfolder, file, _) = self.get_paths(id)?;

        // check if it exists and is a dir...otherwise create the dir
        if subfolder.try_exists()? {
            if !subfolder.is_dir() {
                return Err(FsStorageError::NotDir(subfolder).into());
            }
        } else {
            fs::create_dir_all(&subfolder)?;
            debug!("fsmultikey_map: Created subfolder at: {}", subfolder.display());
        }

        // store the Cid in the filesystem
        debug!("fsmultikey_map: Storing Cid at: {}", file.display());

        // try to get the existing cid value
        let prev_cid = self.get(id).ok();

        // securely create a temporary file. its name begins with "." so that if something goes
        // wrong, the temporary file will be cleaned up by a future GC pass
        let mut temp = tempfile::Builder::new()
            .suffix(&format!(".{}", eid))
            .tempfile_in(&subfolder)?;

        // write the contents to the file
        let data: Vec<u8> = cid.clone().into();
        temp.write_all(data.as_ref())?;

        // atomically rename/move it to the correct location
        temp.persist(&file)?;

        Ok(prev_cid)
    }

    fn rm(&self, id: &Vlad) -> Result<Cid, Self::Error> {
        // first try to get the value
        let v = self.get(id)?;

        // get the paths
        let (_, subfolder, file, lazy_deleted_file) = self.get_paths(id)?;

        // remove the file if it exists
        if file.try_exists()? && file.is_file() {
            if self.lazy {
                // rename the file instead of remove it
                fs::rename(&file, &lazy_deleted_file)?;
                debug!("fsmultikey_map: Lazy deleted mapping at: {} to {}", file.display(), lazy_deleted_file.display());
            } else {
                // not lazy so delete it
                fs::remove_file(&file)?;
                debug!("fsmultikey_map: Removed mapping at: {}", file.display());
            }
        }

        // remove the subfolder if it is emtpy and we're not lazy
        if subfolder.try_exists()? && subfolder.is_dir() && fs::read_dir(&subfolder)?.count() == 0 && !self.lazy {
            fs::remove_dir(&subfolder)?;
            debug!("fsmultikey_map: Removed subdir at: {}", subfolder.display());
        }

        Ok(v)
    }
}

#[cfg(test)]
mod tests {
    use rand;
    use super::*;
    use multicid::{cid, vlad};
    use multicodec::Codec;
    use multihash::mh;
    use multikey::{mk, Multikey};

    // returns a random Ed25519 secret key as a Multikey
    fn get_mk() -> Multikey {
        let mut rng = rand::rngs::OsRng::default();
        mk::Builder::new_from_random_bytes(Codec::Ed25519Priv, &mut rng)
            .unwrap()
            .try_build()
            .unwrap()
    }

    // returns a Cid for the passed in data
    fn get_cid(b: &[u8]) -> Cid {
        cid::Builder::new(Codec::Cidv1)
            .with_target_codec(Codec::Identity)
            .with_hash(&mh::Builder::new_from_bytes(Codec::Sha3512, b).unwrap().try_build().unwrap())
            .try_build()
            .unwrap()
    }

    // returns a signed vlad
    fn get_vlad(b: &[u8]) -> Vlad {
        let mk = get_mk();
        let cid = get_cid(b);

        vlad::Builder::default()
            .with_signing_key(&mk)
            .with_cid(&cid)
            .try_build()
            .unwrap()
    }

    #[test]
    fn test_builder_lazy() {
        let mut pb = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        pb.push(".fsvladmap1");

        let vm = Builder::new(&pb).try_build().unwrap();
        assert_eq!(vm.root, pb);
        assert_eq!(vm.lazy, true);
        assert_eq!(vm.base_encoding, Base::Base32Z);
        assert!(pb.try_exists().is_ok());
        assert!(pb.is_dir());

        assert!(fs::remove_dir_all(&pb).is_ok());
    }

    #[test]
    fn test_builder_not_lazy() {
        let mut pb = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        pb.push(".fsvladmap2");

        let vm = Builder::new(&pb).not_lazy().try_build().unwrap();
        assert_eq!(vm.root, pb);
        assert_eq!(vm.lazy, false);
        assert_eq!(vm.base_encoding, Base::Base32Z);
        assert!(pb.try_exists().is_ok());
        assert!(pb.is_dir());

        for d in fs::read_dir(&pb).unwrap() {
            assert!(d.is_ok());
            let dir = d.unwrap();
            assert!(dir.file_type().unwrap().is_dir());
        }

        assert!(fs::remove_dir_all(&pb).is_ok());
    }

    #[test]
    fn test_put_lazy() {
        let mut pb = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        pb.push(".fsvladmap3");

        let mut vm = Builder::new(&pb).try_build().unwrap();

        let vlad = get_vlad(b"for great justice!");
        let cid1 = get_cid(b"move every zig!");
        let _ = vm.put(&vlad, &cid1).unwrap();
        let cid2 = vm.get(&vlad).unwrap();

        assert_eq!(cid1, cid2);
        assert!(fs::remove_dir_all(&pb).is_ok());
    }

    #[test]
    fn test_put_not_lazy() {
        let mut pb = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        pb.push(".fsvladmap4");

        let mut vm = Builder::new(&pb).not_lazy().try_build().unwrap();

        let vlad = get_vlad(b"for great justice!");
        let cid1 = get_cid(b"move every zig!");
        let _ = vm.put(&vlad, &cid1).unwrap();
        let cid2 = vm.get(&vlad).unwrap();

        assert_eq!(cid1, cid2);
        assert!(fs::remove_dir_all(&pb).is_ok());
    }

    #[test]
    fn test_rm_lazy() {
        let mut pb = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        pb.push(".fsvladmap5");

        let mut vm = Builder::new(&pb).try_build().unwrap();
        
        let vlad = get_vlad(b"for great justice!");
        let cid1 = get_cid(b"move every zig!");
        let _ = vm.put(&vlad, &cid1).unwrap();

        // get the paths to the subfolder and file created from the put
        let (_, _, file, lazy_deleted_file) = vm.get_paths(&vlad).unwrap();

        // lazy delete the block
        let cid2 = vm.rm(&vlad).unwrap();
        assert_eq!(cid1, cid2);

        // this is lazy so the lazy deleted file should sill be there
        assert!(lazy_deleted_file.try_exists().unwrap());
        // and the file should not be there
        assert!(!file.try_exists().unwrap());
        assert!(fs::remove_dir_all(&pb).is_ok());
    }

    #[test]
    fn test_rm_not_lazy() {
        let mut pb = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        pb.push(".fsvladmap6");

        let mut vm = Builder::new(&pb).not_lazy().try_build().unwrap();
        
        let vlad = get_vlad(b"for great justice!");
        let cid1 = get_cid(b"move every zig!");
        let _ = vm.put(&vlad, &cid1).unwrap();

        // get the paths to the subfolder and file created from the put
        let (_, subfolder, file, lazy_deleted_file) = vm.get_paths(&vlad).unwrap();

        // delete the block
        let cid2 = vm.rm(&vlad).unwrap();
        assert_eq!(cid1, cid2);

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
        pb.push(".fsvladmap7");

        let mut vm = Builder::new(&pb).try_build().unwrap();
        
        let vlad1 = get_vlad(b"for great justice!");
        let cid1 = get_cid(b"move every zig!");
        let _ = vm.put(&vlad1, &cid1).unwrap();
        let vlad2 = get_vlad(b"someday");
        let cid2 = get_cid(b"will come");
        let _ = vm.put(&vlad2, &cid2).unwrap();

        let _ = vm.rm(&vlad1).unwrap();
        let _ = vm.rm(&vlad2).unwrap();

        // lazy delete, check that the file is gone, the lazy delete file and folder still exist
        let (_, subfolder1, file1, lazy_deleted_file1) = vm.get_paths(&vlad1).unwrap();
        assert!(lazy_deleted_file1.try_exists().unwrap());
        assert!(!file1.try_exists().unwrap());
        assert!(subfolder1.try_exists().unwrap());

        // lazy delete, check that the file is gone, the lazy delete file and folder still exist
        let (_, subfolder2, file2, lazy_deleted_file2) = vm.get_paths(&vlad2).unwrap();
        assert!(lazy_deleted_file2.try_exists().unwrap());
        assert!(!file2.try_exists().unwrap());
        assert!(subfolder2.try_exists().unwrap());

        // garbage collect
        vm.gc().unwrap();

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
