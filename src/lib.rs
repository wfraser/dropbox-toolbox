//! Dropbox-Toolbox is a simple, user-friendly SDK for working with Dropbox.
//!
//! This crate builds on the [dropbox-sdk](https://github.com/dropbox/dropbox-sdk-rust) crate, which
//! provides a canonical, complete set of Rust bindings to the Dropbox API, but is somewhat
//! difficult to use due to its low-level nature. This crate aims to be an easier-to-use, more
//! high-level SDK, albeit one with smaller surface area.

#![deny(missing_docs)]

#[macro_use]
extern crate log;

use std::time::Duration;

pub mod content_hash;
pub mod download;
pub mod list;
pub mod upload;

/// The size of a block. This is a Dropbox constant, not adjustable.
pub const BLOCK_SIZE: usize = 4 * 1024 * 1024;

/// Options for how to handle error retries.
#[derive(Clone)]
pub struct RetryOpts {
    /// How many consecutive errors until retries are abandoned and the operation is failed?
    pub max: u32,

    /// Errors are handled with retry and exponential backoff with jitter. The first backoff will
    /// be this long, and subsequent backoffs will each be doubled in length (up to
    /// [`max_backoff`](Self::max_backoff)), until [`max`](Self::max) retries have been attempted,
    /// or the request succeeds.
    pub initial_backoff: Duration,

    /// Exponential backoff duration won't increase past this time.
    pub max_backoff: Duration,
}

impl Default for RetryOpts {
    fn default() -> Self {
        Self {
            max: 3,
            initial_backoff: Duration::from_millis(500), // 0.5 + 1 + 2 = 3.5 secs max (+/- jitter)
            max_backoff: Duration::from_secs(2),
        }
    }
}

impl RetryOpts {
    /// Perform the delay called for by the retry options, or return false if the max number of
    /// retries has been reached.
    pub(crate) fn do_retry(&self, retry: &mut u32, backoff: &mut Duration) -> bool {
        if *retry >= self.max {
            return false;
        }
        std::thread::sleep(jitter(*backoff));
        if *backoff < self.max_backoff {
            *backoff *= 2;
        }
        *retry += 1;
        true
    }
}

// Add a random duration in the range [-duration/4, duration/4].
pub(crate) fn jitter(duration: Duration) -> Duration {
    // The API of the rand crate is nicer, but ring is already in our dependency tree, so use it
    // here instead.
    use ring::rand::{generate, SystemRandom};
    let rng = SystemRandom::new();
    let bytes: [u8; 4] = generate(&rng).unwrap().expose();
    let u = u32::from_ne_bytes(bytes);
    let max = f64::from(u32::MAX);
    let f = f64::from(u) / max / 4.;
    if u.is_multiple_of(2) {
        duration + duration.mul_f64(f)
    } else {
        duration - duration.mul_f64(f)
    }
}
