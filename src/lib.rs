#[macro_use]
extern crate log;

use std::error::Error as StdError;
use std::fmt::{self, Debug, Display, Formatter};

pub mod content_hash;
pub mod list;
pub mod upload;

/// The size of a block. This is a Dropbox constant, not adjustable.
pub const BLOCK_SIZE: usize = 4 * 1024 * 1024;

pub enum Error<E> {
    /// An error returned from the API.
    Api(E),

    /// A client-side error encountered in making an API call.
    Other(dropbox_sdk::Error),
}

trait ResultExt<T, E: StdError> {
    fn combine(self) -> Result<T, Error<E>>;
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

//pub type BoxedError = Box<dyn StdError + Send + Sync>;
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
