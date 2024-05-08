[![](https://img.shields.io/badge/made%20by-Cryptid%20Technologies-gold.svg?style=flat-square)][CRYPTID]
[![](https://img.shields.io/badge/project-provenance-purple.svg?style=flat-square)][PROVENANCE]
[![](https://img.shields.io/badge/project-multiformats-blue.svg?style=flat-square)][MULTIFORMATS]
![](https://github.com/cryptidtech/multicid/actions/workflows/rust.yml/badge.svg)

# Content Addressable

A Rust implementation of content addressable storage abstractions using
[multiformats][MULTIFORMATS] [content identifiers (CID)][CID] as the content
address container.

## Current Status

This crate defaults to Base32Z encoding of CID values when needed. It always
calls back to client code for the construction of CIDs so that the client
chooses which CID version and hash algorithm to use.

At this point the only abstraction is a content addressable block store with an
implementation that uses the local file system for storage.

[CRYPTID]: https://cryptid.tech/
[PROVENANCE]: https://github.com/cryptidtech/provenance-specifications/
[MULTIFORMATS]: https://github.com/multiformats/multiformats/
[CID]: https://docs.ipfs.tech/concepts/content-addressing/
