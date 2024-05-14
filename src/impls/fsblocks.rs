// SPDX-License-Identifier: Apache-2.0
use crate::{Blocks, Error, error::FsBlocksError};
use log::debug;
use multibase::Base;
use multicid::Cid;
use multiutil::BaseEncoded;
use serde::{Deserialize, Serialize};
use std::{fs::{self, File}, io::{Read, Write}, path::{Path, PathBuf}};

/// Filesystem block storage handle
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct FsBlocks {
    /// The root directory
    pub root: PathBuf,
    /// Should folders be created lazily?
    pub lazy: bool,
    /// The base encoding for new CIDs
    #[serde(with = "serde_base")]
    pub base_encoding: Base,
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

impl Blocks for FsBlocks {
    type Error = Error;

    fn get(&self, cid: &Cid) -> Result<Vec<u8>, Self::Error> {
        // get the base encoded CID
        let ecid = BaseEncoded::new(self.base_encoding, cid.clone());

        // get the last char of the encoded CID
        let c = ecid.to_string().chars().last().ok_or(FsBlocksError::InvalidCid(ecid.clone()))?;

        // create a pathbuf to the subfolder
        let mut pb = self.root.clone();
        pb.push(c.to_string());

        // check if it exists and is a dir...otherwise create the dir
        let exists = pb.try_exists()?;
        if exists {
            if !pb.is_dir() {
                return Err(FsBlocksError::NotDir(pb).into());
            }
        } else {
            return Err(FsBlocksError::NoSuchBlock(ecid).into());
        }

        // store the block in the filesystem
        pb.push(&ecid.to_string());
        debug!("Getting block from: {}", pb.display());
        let mut f = File::open(&pb)?;
        let mut data = Vec::default();
        f.read_to_end(&mut data)?;
        Ok(data)
    }

    fn put<D, F>(&mut self, data: &D, get_cid: F) -> Result<Cid, Self::Error>
    where
        D: AsRef<[u8]>,
        F: Fn(&D) -> Result<Cid, Self::Error>
    {
        // call the callback for calculating the CID
        let cid = get_cid(data)?;

        // get the base encoded CID
        let ecid = BaseEncoded::new(self.base_encoding, cid.clone());

        // get the first char of the encoded CID
        let c = ecid.to_string().chars().last().ok_or(FsBlocksError::InvalidCid(ecid.clone()))?;

        // create a pathbuf to the subfolder
        let mut pb = self.root.clone();
        pb.push(c.to_string());

        // check if it exists and is a dir...otherwise create the dir
        let exists = pb.try_exists()?;
        if exists {
            if !pb.is_dir() {
                return Err(FsBlocksError::NotDir(pb).into());
            }
        } else {
            fs::create_dir_all(pb.clone())?;
        }

        // store the block in the filesystem
        pb.push(&ecid.to_string());
        debug!("Storing block at: {}", pb.display());
        let mut f = File::create(&pb)?;
        f.write_all(data.as_ref())?;
        Ok(cid)
    }
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
        _ => Err(FsBlocksError::UnsupportedBaseEncoding(base.clone()).into())
    }
}

/// Builder for a FsBlock instance
#[derive(Clone, Debug, Default)]
pub struct Builder {
    root: PathBuf,
    lazy: bool,
    base_encoding: Option<Base>,
}

impl Builder {
    /// create a new builder from the root path, this defaults to lazy
    pub fn new(root: &Path) -> Self {
        debug!("fsblocks::Builder::new({})", root.display());
        Builder {
            root: root.to_path_buf(),
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

        // create the root directory
        let root = self.root.clone();
        if !root.exists() {
            debug!("fsblocks: creating root dir at {}", root.display());
            fs::create_dir_all(&root)?;
        }
        debug!("fsblocks: root dir exists");

        if !self.lazy {
            // construct the directory structure using the alphabent of the base encoder
            let symbols = encoding_symbols(&base_encoding)?;
            for c in symbols.chars() {
                let mut p = root.clone();
                p.push(c.to_string());
                if !p.exists() {
                    debug!("fsblocks: creating {}", p.display());
                    fs::create_dir_all(&p)?;
                }
            }
        }
        debug!("fsblocsk: symbol dirs exists (might be lazy created)");

        Ok(FsBlocks {
            root: root.clone(),
            lazy: self.lazy,
            base_encoding
        })
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
        pb.push(".tmp1");

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
        pb.push(".tmp2");

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

    #[test]
    fn test_put_lazy() {
        let mut pb = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        pb.push(".tmp3");

        let mut blocks = Builder::new(&pb).try_build().unwrap();
        
        let v1 = b"for great justice!".to_vec();
        let cid = blocks.put(&v1, |data| -> Result<Cid, Error> {
            let mh = mh::Builder::new_from_bytes(Codec::Blake3, data.clone())?
                .try_build()?;
            let cid = cid::Builder::new(Codec::Cidv1)
                .with_target_codec(Codec::Identity)
                .with_hash(&mh)
                .try_build()?;
            Ok(cid)
        }).unwrap();
        
        let v2 = blocks.get(&cid).unwrap();
        assert_eq!(v1, v2);

        assert!(fs::remove_dir_all(&pb).is_ok());
    }

    #[test]
    fn test_verify_not_lazy() {
        let mut pb = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        pb.push(".tmp4");

        let mut blocks = Builder::new(&pb).not_lazy().try_build().unwrap();
        
        let v1 = b"move every zig!".to_vec();
        let cid = blocks.put(&v1, |data| -> Result<Cid, Error> {
            let mh = mh::Builder::new_from_bytes(Codec::Blake3, data.clone())?
                .try_build()?;
            let cid = cid::Builder::new(Codec::Cidv1)
                .with_target_codec(Codec::Identity)
                .with_hash(&mh)
                .try_build()?;
            Ok(cid)
        }).unwrap();
        
        let v2 = blocks.get(&cid).unwrap();
        assert_eq!(v1, v2);

        assert!(fs::remove_dir_all(&pb).is_ok());
    }
}
