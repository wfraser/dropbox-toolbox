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

/// Extension methods for Result
pub trait ResultExt<T> {
    /// For a result with a [`dropbox_sdk::Error`] error type, convert the error to [`dropbox_sdk::BoxedError`] so different error types can be combined into one result type.
    fn boxed_err(self) -> Result<T, dropbox_sdk::BoxedError>;
}

impl<T, E> ResultExt<T> for Result<T, dropbox_sdk::Error<E>>
where
    E: std::error::Error + Send + Sync + 'static,
{
    fn boxed_err(self) -> Result<T, dropbox_sdk::BoxedError> {
        self.map_err(dropbox_sdk::Error::boxed)
    }
}
