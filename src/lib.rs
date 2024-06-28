//! Dropbox-Toolbox is a simple, user-friendly SDK for working with Dropbox.
//!
//! This crate builds on the [dropbox-sdk](https://github.com/dropbox/dropbox-sdk-rust) crate, which
//! provides a canonical, complete set of Rust bindings to the Dropbox API, but is somewhat
//! difficult to use due to its low-level nature. This crate aims to be an easier-to-use, more
//! high-level SDK, albeit one with smaller surface area.

#![deny(missing_docs)]

#[macro_use]
extern crate log;

pub mod content_hash;
pub mod list;
pub mod upload;

/// The size of a block. This is a Dropbox constant, not adjustable.
pub const BLOCK_SIZE: usize = 4 * 1024 * 1024;
