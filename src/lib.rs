//! Dropbox-Toolbox is a simple, user-friendly SDK for working with Dropbox.
//!
//! This crate builds on the [dropbox-sdk](https://github.com/dropbox/dropbox-sdk-rust) crate, which
//! provides a canonical, complete set of Rust bindings to the Dropbox API, but is somewhat
//! difficult to use due to its low-level nature. This crate aims to be an easier-to-use, more
//! high-level SDK, albeit one with smaller surface area.

#![deny(missing_docs)]

#[macro_use]
extern crate log;

use std::error::Error as StdError;
use std::fmt::{self, Debug, Display, Formatter};

pub mod content_hash;
pub mod list;
pub mod upload;

/// The size of a block. This is a Dropbox constant, not adjustable.
pub const BLOCK_SIZE: usize = 4 * 1024 * 1024;

/// An error from calling the Dropbox API, either client-side or server-side.
pub enum Error<E> {
    /// An error returned from the Dropbox API.
    Api(E),

    /// A client-side error encountered in making an API call.
    Other(dropbox_sdk::Error),
}

/// The Dropbox SDK returns nested results of the form `Result<Result<T, E>, dropbox_sdk::Error>`,
/// where the outer result is used to report general client-side errors, and the inner result
/// reports success or strongly-typed error results from the API itself.
///
/// This extension trait is for translating these nested results into the [`Error`] and
/// [`BoxedError`] types in this crate.
pub trait ResultExt<T, E: StdError> {
    /// Combine a nested `Result<Result<T, E>, dropbox_sdk::Error>` from the Dropbox SDK into a
    /// result which uses the [`Error`] enum in this crate.
    fn combine(self) -> Result<T, Error<E>>;

    /// Combine a nested result into a result which uses the [`BoxedError`] type in this crate.
    /// Useful when generics are not desired.
    fn boxed(self) -> Result<T, BoxedError>;
}

impl<T, E: StdError + Send + Sync + 'static> ResultExt<T, E>
    for Result<Result<T, E>, dropbox_sdk::Error>
{
    fn combine(self) -> Result<T, Error<E>> {
        match self {
            Ok(Ok(v)) => Ok(v),
            Ok(Err(e)) => Err(Error::Api(e)),
            Err(e) => Err(Error::Other(e)),
        }
    }

    fn boxed(self) -> Result<T, BoxedError> {
        match self {
            Ok(Ok(v)) => Ok(v),
            Ok(Err(e)) => Err(BoxedError(Box::new(e))),
            Err(e) => Err(BoxedError(Box::new(e))),
        }
    }
}

impl<T, E: StdError + Send + Sync + 'static> ResultExt<T, E> for Result<T, Error<E>> {
    fn combine(self) -> Result<T, Error<E>> {
        self
    }

    fn boxed(self) -> Result<T, BoxedError> {
        match self {
            Ok(v) => Ok(v),
            Err(Error::Api(e)) => Err(BoxedError(Box::new(e))),
            Err(Error::Other(e)) => Err(BoxedError(Box::new(e))),
        }
    }
}

impl<E: StdError> Debug for Error<E> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Api(ref e) => Debug::fmt(e, f),
            Self::Other(ref e) => Debug::fmt(e, f),
        }
    }
}

impl<E: StdError> Display for Error<E> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Api(ref e) => Display::fmt(e, f),
            Self::Other(ref e) => Display::fmt(e, f),
        }
    }
}

impl<E: StdError> StdError for Error<E> {
    fn cause(&self) -> Option<&dyn StdError> {
        match self {
            Self::Api(e) => Some(e),
            Self::Other(ref e) => Some(e),
        }
    }
}

/// Combines the various kinds of errors in [`Error`] into a boxed error without type variables,
/// which can be downcasted to specific errors if desired.
pub struct BoxedError(Box<dyn StdError + Send + Sync>);

impl std::ops::Deref for BoxedError {
    type Target = Box<dyn StdError + Send + Sync>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<E: StdError + Send + Sync + 'static> From<Error<E>> for BoxedError {
    fn from(value: Error<E>) -> Self {
        BoxedError(match value {
            Error::Api(e) => Box::new(e),
            Error::Other(e) => Box::new(e),
        })
    }
}

impl Debug for BoxedError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        Debug::fmt(&self.0, f)
    }
}

impl Display for BoxedError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(&self.0, f)
    }
}

impl StdError for BoxedError {}
