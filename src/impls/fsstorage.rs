// SPDX-License-Identifier: Apache-2.0
use crate::{Error, error::FsStorageError};
use log::debug;
use multibase::Base;
use multiutil::{BaseEncoded, BaseEncoder, DetectedEncoder, EncodingInfo};
use serde::{Deserialize, Serialize};
use std::{fs, marker::PhantomData, path::{Path, PathBuf}};

/// Filesystem block storage handle
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct FsStorage<T>
where
    T: EncodingInfo + ?Sized
{
    /// The root directory
    pub root: PathBuf,
    /// Should folders be created lazily?
    pub lazy: bool,
    /// The base encoding for new CIDs
    #[serde(with = "serde_base")]
    pub base_encoding: Base,

    // phantoms
    _t: PhantomData<T>,
}

impl<T> EncodingInfo for FsStorage<T>
where
    T: EncodingInfo
{
    fn preferred_encoding() -> Base {
        DetectedEncoder::preferred_encoding(T::preferred_encoding())
    }

    fn encoding(&self) -> Base {
        self.base_encoding
    }
}

impl<T> FsStorage<T>
where
    T: Clone + EncodingInfo + Into<Vec<u8>>
{
    /// garbage collect the block storage to remove any lazy deleted files and empty subfolders
    pub fn gc(&mut self) -> Result<(), Error> {
        for subfolder in &Self::subfolders(Some(self.encoding()), &self.root)? {
            if !subfolder.try_exists()? {
                continue;
            }
            for file in fs::read_dir(subfolder)? {
                let file = file?;
                if file.file_name().to_string_lossy().starts_with('.') {
                    fs::remove_file(&file.path())?;
                    debug!("fsstorage: GC'd file {}", file.path().display());
                }
            }
            if fs::read_dir(subfolder)?.count() == 0 {
                fs::remove_dir(subfolder)?;
                debug!("fsstorage: GC'd subfolder {}", subfolder.display());
            }
        }
        Ok(())
    }

    /// get an iterator over the subfolders given the base encoding
    pub fn subfolders<P: AsRef<Path>>(base_encoding: Option<Base>, root: P) -> Result<Vec<PathBuf>, Error> {
        let base_encoding = base_encoding.unwrap_or(FsStorage::<T>::preferred_encoding());

        // create the root directory
        if !root.as_ref().try_exists()? {
            debug!("fsstorage: creating root dir at {}", root.as_ref().display());
            fs::create_dir_all(&root)?;
        }
        debug!("fsstorage: root dir exists");

        // construct the directory structure using the alphabent of the base encoder
        let symbols = Self::encoding_symbols(&base_encoding)?;
        Ok(symbols.chars().map(|c| {
            let mut p = root.as_ref().to_path_buf();
            p.push(c.to_string());
            p
        }).collect())
    }

    fn encoding_symbols(base: &Base) -> Result<String, Error> {
        match base {
            Base::Base2 => Ok("01".into()),
            Base::Base8 => Ok("01234567".into()),
            Base::Base10 => Ok("0123456789".into()),
            Base::Base16Lower => Ok("0123456789abcdef".into()),
            Base::Base16Upper => Ok("0123456789ABCDEF".into()),
            Base::Base32Lower | Base::Base32PadLower => Ok("abcdefghijklmnopqrstuvwxyz234567".into()),
            Base::Base32Upper | Base::Base32PadUpper => Ok("ABCDEFGHIJKLMNOPQRSTUVWXYZ234567".into()),
            Base::Base32HexLower | Base::Base32HexPadLower => Ok("0123456789abcdefghijklmnopqrstuv".into()),
            Base::Base32HexUpper | Base::Base32HexPadUpper => Ok("0123456789ABCDEFGHIJKLMNOPQRSTUV".into()),
            Base::Base32Z => Ok("ybndrfg8ejkmcpqxot1uwisza345h769".into()),
            Base::Base36Lower => Ok("0123456789abcdefghijklmnopqrstuvwxyz".into()),
            Base::Base36Upper => Ok("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ".into()),
            Base::Base58Flickr => Ok("123456789abcdefghijkmnopqrstuvwxyzABCDEFGHJKLMNPQRSTUVWXYZ".into()),
            Base::Base58Btc => Ok("123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz".into()),
            Base::Base64 |
            Base::Base64Pad |
            Base::Base64Url |
            Base::Base64UrlPad => Ok("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_".into()),
            _ => Err(FsStorageError::UnsupportedBaseEncoding(*base).into())
        }
    }

    pub(crate) fn get_paths(&self, id: &T) -> Result<(BaseEncoded<T, DetectedEncoder>, PathBuf, PathBuf, PathBuf), Error> {
        let eid = self.encode(id)?;
        let subfolder = self.get_subfolder(&eid)?;
        let file = self.get_file(&subfolder, &eid)?;
        let lazy_deleted_file = self.get_lazy_deleted_file(&subfolder, &eid)?;
        Ok((eid, subfolder, file, lazy_deleted_file))
    }

    fn encode(&self, id: &T) -> Result<BaseEncoded<T, DetectedEncoder>, Error> {
        Ok(BaseEncoded::<T, DetectedEncoder>::new(self.base_encoding, id.clone()))
    }

    fn get_subfolder(&self, eid: &BaseEncoded<T, DetectedEncoder>) -> Result<PathBuf, Error> {
        // get the middle char of the encoded CID
        let s = format!("{eid}");
        let l = s.len();
        let c = s.chars().nth_back(l >> 1).ok_or(FsStorageError::InvalidId(eid.to_string()))?;

        // create a pathbuf to the subfolder
        let mut pb = self.root.clone();
        pb.push(c.to_string());

        Ok(pb)
    }

    fn get_file<P: AsRef<Path>>(&self, subfolder: P, eid: &BaseEncoded<T, DetectedEncoder>) -> Result<PathBuf, Error> {
        let mut pb = subfolder.as_ref().to_path_buf();
        pb.push(&eid.to_string());
        Ok(pb)
    }

    fn get_lazy_deleted_file<P: AsRef<Path>>(&self, subfolder: P, eid: &BaseEncoded<T, DetectedEncoder>) -> Result<PathBuf, Error> {
        let mut pb = subfolder.as_ref().to_path_buf();
        pb.push(&format!(".{}", eid));
        Ok(pb)
    }
}

pub(crate) mod serde_base {
    use multibase::Base;
    use serde::{Deserialize, Deserializer, Serializer};

    pub(crate) fn serialize<S>(v: &Base, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_char(v.code())
    }

    pub(crate) fn deserialize<'de, D>(deserializer: D) -> Result<Base, D::Error>
    where
        D: Deserializer<'de>,
    {
        let c = char::deserialize(deserializer)?;
        let base = Base::from_code(c).map_err(serde::de::Error::custom)?;
        Ok(base)
    }
}

/// Builder for a FsStorage instance
#[derive(Clone, Debug, Default)]
pub struct Builder<T> 
where
    T: EncodingInfo + Clone + ?Sized
{
    root: PathBuf,
    lazy: bool,
    base_encoding: Option<Base>,
    _t: PhantomData<T>,
}

impl<T> Builder<T>
where
    T: Clone + EncodingInfo + Into<Vec<u8>>
{
    /// create a new builder from the root path, this defaults to lazy
    pub fn new<P: AsRef<Path>>(root: P) -> Self {
        debug!("fsstorage::Builder::new({})", root.as_ref().display());
        Builder {
            root: root.as_ref().to_path_buf(),
            lazy: true,
            base_encoding: None,
            _t: PhantomData,
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
    pub fn try_build(&self) -> Result<FsStorage<T>, Error> {
        let lazy = self.lazy;
        let base_encoding = self.base_encoding.unwrap_or(FsStorage::<T>::preferred_encoding());

        // create the root directory
        let root = self.root.clone();
        if !root.try_exists()? {
            debug!("fsstorage: Creating root folder at {}", root.display());
            fs::create_dir_all(&root)?;
        }
        debug!("fsstorage: Root dir exists");

        if !self.lazy {
            // construct the directory structure using the alphabent of the base encoder
            for subfolder in &FsStorage::<T>::subfolders(self.base_encoding, &root)? {
                if !subfolder.try_exists()? {
                    debug!("fsstorage: Creating subfolder {}", subfolder.display());
                    fs::create_dir_all(subfolder)?;
                }
            }
        }

        Ok(FsStorage {
            root,
            lazy,
            base_encoding,
            _t: PhantomData,
        })
    }
}


