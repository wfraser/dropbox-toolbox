//! Functions for downloading files.

use crate::{jitter, RetryOpts};
use dropbox_sdk::files::{self, DownloadArg, DownloadError, FileMetadata};
use dropbox_sdk::{Error, UserAuthClient};
use std::io::{self, Read};
use std::sync::Arc;
use std::thread::sleep;

/// A file download in progress.
pub struct DownloadSession<C> {
    client: Arc<C>,
    retry: RetryOpts,
    arg: DownloadArg,
    range_start: Option<u64>,
    range_end: Option<u64>,
    metadata: FileMetadata,
    body: Box<dyn Read>,
    content_length: u64,
    cursor: u64,
}

/// Download a file, with configurable retries on errors.
pub fn download<C: UserAuthClient + Send + Sync>(
    client: Arc<C>,
    retry: RetryOpts,
    arg: DownloadArg,
    range_start: Option<u64>,
    range_end: Option<u64>,
) -> Result<DownloadSession<C>, Error<DownloadError>> {
    let mut session = DownloadSession {
        client,
        retry,
        arg,
        range_start,
        range_end,
        metadata: FileMetadata::new(
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
            0,
        ),
        body: Box::new(io::empty()),
        content_length: 0,
        cursor: 0,
    };

    session.request()?;

    Ok(session)
}

impl<C: UserAuthClient + Send + Sync> DownloadSession<C> {
    /// Get the metadata of the file.
    pub fn metadata(&self) -> &FileMetadata {
        &self.metadata
    }

    /// Get the content-length header returned by the API.
    pub fn content_length(&self) -> u64 {
        self.content_length
    }

    /// The number of bytes read so far.
    pub fn bytes_read(&self) -> u64 {
        self.cursor
    }

    fn request(&mut self) -> Result<(), Error<DownloadError>> {
        let range_start = match self.range_start {
            Some(start) => Some(start + self.cursor),
            None => Some(self.cursor),
        };
        let resp = files::download(self.client.as_ref(), &self.arg, range_start, self.range_end)?;
        self.body = resp
            .body
            .ok_or_else(|| Error::UnexpectedResponse("response has no body".to_owned()))?;
        self.content_length = resp.content_length.ok_or_else(|| {
            Error::UnexpectedResponse("response has no content-length".to_owned())
        })?;
        Ok(())
    }
}

impl<C: UserAuthClient + Send + Sync> Read for DownloadSession<C> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let mut backoff = self.retry.initial_backoff;
        let mut err: Option<dropbox_sdk::Error<DownloadError>> = None;
        for retry in 0.. {
            if let Some(e) = err.take() {
                error!("download error: {e}");
                if retry + 1 == self.retry.max {
                    return Err(io::Error::other(e));
                }
                sleep(jitter(backoff));
                if backoff < self.retry.max_backoff {
                    backoff *= 2;
                } else {
                    return Err(io::Error::other(e));
                }
                err = self.request().err();
                continue;
            }

            err = match self.body.read(buf) {
                Ok(n) => {
                    self.cursor += n as u64;
                    return Ok(n);
                }
                Err(e) => Some(Error::HttpClient(Box::new(e))),
            };
        }
        unreachable!()
    }
}
