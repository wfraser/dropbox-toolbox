//! Helpers for calculating Dropbox Content Hashes.
//!
//! A Dropbox Content Hash is the result of taking a file, dividing it into 4 MiB blocks,
//! calculating the SHA-256 of each block, concatenating those hashes, and taking the SHA-256 of
//! that.
//!
//! Dropbox keeps a Content Hash of each file, which can be quickly obtained as part of the
//! [`Metadata`](dropbox_sdk::files::Metadata) of a file, which can be used to verify the integrity of an
//! upload or download.

use std::fmt::Write;
use std::io::{self, Read};

use ring::digest::Context as HashContext;
use ring::digest::SHA256;

use crate::BLOCK_SIZE;

/// A ContentHash is a SHA-256, and is 256 bytes long.
pub const OUTPUT_SIZE: usize = 256 / 8;

/// ContentHash is a data integrity check used by the Dropbox API.
#[derive(Clone)]
pub struct ContentHash {
    ctx: HashContext,
    block_ctx: HashContext,
    partial: usize,
}

impl ContentHash {
    /// Create a new empty ContentHash.
    pub fn new() -> Self {
        ContentHash {
            ctx: HashContext::new(&SHA256),
            block_ctx: HashContext::new(&SHA256),
            partial: 0,
        }
    }

    /// Update the content hash with some data.
    pub fn update(&mut self, mut bytes: &[u8]) {
        if self.partial != 0 {
            let partial_needed = BLOCK_SIZE - self.partial;
            let (first, rem) = if partial_needed < bytes.len() {
                bytes.split_at(partial_needed)
            } else {
                (bytes, &[][..])
            };
            self.block_ctx.update(first);
            self.partial += first.len();
            if self.partial == BLOCK_SIZE {
                self.finish_block();
            } else {
                assert!(rem.is_empty());
                return;
            }
            bytes = rem;
        }

        for block in bytes.chunks(BLOCK_SIZE) {
            self.block_ctx.update(block);
            if block.len() < BLOCK_SIZE {
                self.partial = block.len();
            } else {
                self.finish_block();
            }
        }
    }

    /// Read and hash a byte stream.
    pub fn read_stream(&mut self, mut stream: impl Read) -> io::Result<()> {
        let mut buf = vec![0u8; BLOCK_SIZE];
        loop {
            let nread = match stream.read(&mut buf) {
                Ok(0) => break,
                Ok(n) => n,
                Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(e),
            };
            self.update(&buf[0..nread]);
        }
        Ok(())
    }

    /// Finish the Content Hash and return the bytes.
    pub fn finish(mut self) -> [u8; OUTPUT_SIZE] {
        if self.partial != 0 {
            self.finish_block();
        }
        let mut out = [0u8; OUTPUT_SIZE];
        out.copy_from_slice(self.ctx.finish().as_ref());
        out
    }

    /// Finish the Content Hash and return it as a hexadecimal string.
    pub fn finish_hex(self) -> String {
        hex(&self.finish())
    }

    fn finish_block(&mut self) {
        let block_hash = std::mem::replace(&mut self.block_ctx, HashContext::new(&SHA256)).finish();
        self.ctx.update(block_hash.as_ref());
        self.partial = 0;
    }
}

impl Default for ContentHash {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: AsRef<[u8]>> From<T> for ContentHash {
    fn from(src: T) -> Self {
        let mut hash = Self::new();
        hash.update(src.as_ref());
        hash
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().fold(String::new(), |mut s, byte| {
        // std::fmt::Write for String does not return errors.
        write!(&mut s, "{:02x}", byte).unwrap();
        s
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_bytes() {
        let ctx1 = ContentHash::new();
        let r1 = ctx1.finish_hex();

        let mut ctx2 = ContentHash::new();
        ctx2.update(&[]);
        let r2 = ctx2.finish_hex();

        assert_eq!(&r1, &r2);
        assert_eq!(
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            &r1
        );
    }

    #[test]
    fn less_than_one_block() {
        let mut ctx = ContentHash::new();
        ctx.update(b"hello");
        assert_eq!(5, ctx.partial);
        assert_eq!(
            "9595c9df90075148eb06860365df33584b75bff782a510c6cd4883a419833d50",
            &ctx.finish_hex()
        );
    }

    #[test]
    fn tiny_updates() {
        let mut ctx = ContentHash::new();
        ctx.update(b"h");
        ctx.update(b"e");
        ctx.update(b"l");
        ctx.update(b"l");
        ctx.update(b"o");
        assert_eq!(
            "9595c9df90075148eb06860365df33584b75bff782a510c6cd4883a419833d50",
            &ctx.finish_hex()
        );
    }

    #[test]
    fn exactly_one_block() {
        let mut ctx = ContentHash::new();
        ctx.update(&[30; BLOCK_SIZE]);
        assert_eq!(0, ctx.partial);
        assert_eq!(
            "1114501b241325c24970e0cd0b6416d80284085151e2980747ccecc4e0c156e6",
            &ctx.finish_hex()
        );
    }

    #[test]
    fn one_block_and_a_little_bit_more() {
        let mut ctx = ContentHash::new();
        ctx.update(&[30; BLOCK_SIZE + 1]);
        assert_eq!(1, ctx.partial);
        assert_eq!(
            "5b1d15f99119b9138a887c27d1b246cf6c584621fc75c42edd27c3d962835d4f",
            &ctx.finish_hex()
        );
    }

    #[test]
    fn exactly_two_blocks() {
        let mut ctx = ContentHash::new();
        ctx.update(&[30; 2 * BLOCK_SIZE]);
        assert_eq!(0, ctx.partial);
        assert_eq!(
            "aa562efb265c604214e4626717330e15be16f2daaabfe5d7d2c22f3e88cbc268",
            &ctx.finish_hex()
        );
    }

    #[test]
    fn exactly_two_blocks_separately() {
        let mut ctx = ContentHash::new();
        ctx.update(&[30; BLOCK_SIZE]);
        ctx.update(&[30; BLOCK_SIZE]);
        assert_eq!(
            "aa562efb265c604214e4626717330e15be16f2daaabfe5d7d2c22f3e88cbc268",
            &ctx.finish_hex()
        );
    }

    #[test]
    fn partial_blocks() {
        let mut ctx = ContentHash::new();
        ctx.update(&[30; BLOCK_SIZE / 2]); // 1/2
        ctx.update(&[30; BLOCK_SIZE]); // 1-1/2
        ctx.update(&[30; BLOCK_SIZE / 2]); // 2
        assert_eq!(
            "aa562efb265c604214e4626717330e15be16f2daaabfe5d7d2c22f3e88cbc268",
            &ctx.finish_hex()
        );
    }

    #[test]
    fn partial_blocks_2() {
        let mut ctx = ContentHash::new();
        ctx.update(&[30; BLOCK_SIZE / 4]); // 1/4
        ctx.update(&[30; BLOCK_SIZE / 2]); // 3/4
        ctx.update(&[30; BLOCK_SIZE / 2]); // 1-1/4
        ctx.update(&[30; BLOCK_SIZE / 2]); // 1-3/4
        ctx.update(&[30; BLOCK_SIZE / 4]); // 2
        assert_eq!(
            "aa562efb265c604214e4626717330e15be16f2daaabfe5d7d2c22f3e88cbc268",
            &ctx.finish_hex()
        );
    }
}
