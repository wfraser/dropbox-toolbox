//! Functions for uploading files.

use std::collections::HashMap;
use std::io::Read;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::{AtomicU64, Ordering::SeqCst};
use std::sync::{Arc, Mutex};
use std::thread::sleep;
use std::time::{Duration, Instant};

use crate::content_hash::ContentHash;
use crate::BLOCK_SIZE;
use dropbox_sdk::{BoxedError, Error};
use dropbox_sdk::files::{self, UploadSessionAppendError, UploadSessionFinishError};
use dropbox_sdk::UserAuthClient;

/// Options for how to perform uploads.
#[derive(Clone)]
pub struct UploadOpts {
    /// How many blocks to upload in parallel.
    pub parallelism: usize,

    /// How many blocks (of [`BLOCK_SIZE`] bytes each) are uploaded in each request.
    ///
    /// Uploading multiple blocks per request reduces the number of requests needed to complete the
    /// upload and can reduce overhead and help avoid running into rate limits, at the cost of
    /// increasing the cost of a request that has to be retried in the event of an error.
    pub blocks_per_request: usize,

    /// How many consecutive errors until retries are abandoned and the upload is failed?
    pub retry_count: u32,

    /// Errors when uploading are handled with retry and exponential backoff with jitter. The first
    /// backoff will be this long, and subsequent backoffs will each be doubled in length (up to
    /// [`max_backoff_time`](Self::max_backoff_time)), until [`retry_count`](Self::retry_count)
    /// retries have been attempted, or the upload request succeeds.
    pub initial_backoff_time: Duration,

    /// Exponential backoff duration won't increase past this time.
    pub max_backoff_time: Duration,

    /// An optional callback to periodically receive progress updates as the file uploads.
    pub progress_handler: Option<Arc<Box<dyn ProgressHandler>>>,
}

impl Default for UploadOpts {
    fn default() -> Self {
        Self {
            parallelism: 20,
            blocks_per_request: 2,
            retry_count: 3,
            initial_backoff_time: Duration::from_millis(500), // 0.5 + 1 + 2 = 3.5 secs max (+/- jitter)
            max_backoff_time: Duration::from_secs(2),
            progress_handler: None,
        }
    }
}

/// Implement to receive periodic progress updates as a file uploads.
pub trait ProgressHandler: Sync + Send {
    /// Invoked with the following parameters:
    /// - total bytes uploaded so far
    /// - the rate (bytes/sec) of the most recent chunk uploaded
    /// - the overall rate (bytes/sec) of the whole upload
    fn update(&self, bytes_uploaded: u64, instant_rate: f64, overall_rate: f64);
}

/// Parameters to resume an incomplete upload.
#[derive(Debug, Clone)]
pub struct UploadResume {
    /// The upload session ID.
    pub session_id: String,

    /// The offset in bytes to resume from.
    pub start_offset: u64,
}

/// An upload session for a file.
pub struct UploadSession<C: UserAuthClient + Send + Sync + 'static> {
    client: Arc<C>,
    inner: Arc<SessionInner>,
}

struct SessionInner {
    session_id: String,
    start_offset: u64,
    bytes_transferred: AtomicU64,
    completion: Mutex<CompletionTracker>,
}

impl<C: UserAuthClient + Send + Sync + 'static> UploadSession<C> {
    /// Make a new upload session.
    pub fn new(client: Arc<C>) -> Result<Self, Error<files::UploadSessionStartError>> {
        let session_id = files::upload_session_start(
            client.as_ref(),
            &files::UploadSessionStartArg::default()
                .with_session_type(files::UploadSessionType::Concurrent),
            &[],
        )?
        .session_id;

        Ok(Self {
            client,
            inner: Arc::new(SessionInner {
                session_id,
                start_offset: 0,
                bytes_transferred: AtomicU64::new(0),
                completion: Mutex::new(CompletionTracker::default()),
            }),
        })
    }

    /// Resume a pre-existing (i.e. interrupted) upload session.
    pub fn resume(client: Arc<C>, resume: UploadResume) -> Self {
        Self {
            client,
            inner: Arc::new(SessionInner {
                session_id: resume.session_id,
                start_offset: resume.start_offset,
                bytes_transferred: AtomicU64::new(0),
                completion: Mutex::new(CompletionTracker::resume_from(resume.start_offset)),
            }),
        }
    }

    /// Upload the given stream to the upload session, using the given
    /// [upload parameters](UploadOpts). This may only be called once for a given
    /// [`UploadSession`].
    ///
    /// This blocks the current thread until the entire source has been transferred, or an error
    /// occurs.
    ///
    /// The return value is the number of bytes uploaded, or an error.
    ///
    /// If the upload fails, call [`UploadSession::get_resume`] to get the resume parameters which
    /// can be passed to [`UploadSession::resume`] to make a new [`UploadSession`] which can be
    /// used to retry the upload without re-uploading all the data.
    pub fn upload(&self, mut source: impl Read, opts: UploadOpts) -> Result<u64, BoxedError> {
        let closed = Arc::new(AtomicBool::new(false));
        let start_time = Instant::now();
        let result = {
            let client = self.client.clone();
            let inner = self.inner.clone();
            let opts = opts.clone();
            let closed = closed.clone();
            parallel_reader::read_stream_and_process_chunks_in_parallel(
                &mut source,
                BLOCK_SIZE * opts.blocks_per_request,
                opts.parallelism,
                Arc::new(
                    move |block_offset,
                          data: &[u8]|
                          -> Result<(), Error<UploadSessionAppendError>> {
                        let mut append_arg = inner
                            .append_arg(block_offset)
                            .with_content_hash(ContentHash::from(data).finish_hex());
                        if data.len() != BLOCK_SIZE * opts.blocks_per_request {
                            // This must be the last block. Only the last one is allowed to be not 4 MiB
                            // exactly.
                            append_arg.close = true;
                            closed.store(true, SeqCst);
                        }
                        let result = Self::upload_block_with_retry(
                            client.as_ref(),
                            inner.as_ref(),
                            &append_arg,
                            data,
                            start_time,
                            &opts,
                        );
                        if result.is_ok() {
                            inner.mark_block_uploaded(block_offset, data.len() as u64);
                        }
                        result
                    },
                ),
            )
        };

        result.map_err(|e| match e {
            parallel_reader::Error::Read(e) => Error::HttpClient(e.into()),
            parallel_reader::Error::Process {
                chunk_offset: _,
                error,
            } => error.boxed(),
        })?;

        let final_len = self.inner.complete_up_to();
        // If we didn't close it above, we need to upload an empty buffer now to mark the session as
        // closed.
        if !closed.load(SeqCst) {
            let append_arg = self.inner.append_arg(final_len).with_close(true);
            if let Err(e) = Self::upload_block_with_retry(
                self.client.as_ref(),
                self.inner.as_ref(),
                &append_arg,
                &[],
                start_time,
                &opts,
            ) {
                warn!("failed to close session: {}", e);
                // But don't error out; try committing anyway. It could be we're resuming a file
                // where we already closed it out but failed to commit.
            }
        }

        Ok(final_len)
    }

    /// After calling [`UploadSession::upload`], commit the data to a file.
    pub fn commit(
        &self,
        commit_info: files::CommitInfo,
    ) -> Result<files::FileMetadata, Error<UploadSessionFinishError>> {
        let finish = self.inner.commit_arg(commit_info);

        let mut errors = 0;
        loop {
            match files::upload_session_finish(self.client.as_ref(), &finish, &[]) {
                Ok(file_metadata) => {
                    info!(
                        "Upload succeeded: {}",
                        file_metadata.path_display.as_deref().unwrap_or("?")
                    );
                    return Ok(file_metadata);
                }
                Err(e) => {
                    errors += 1;
                    if errors == 3 {
                        error!("Error committing upload: {e}, failing.");
                        return Err(e);
                    } else {
                        warn!("Error committing upload: {e}, retrying.");
                        sleep(Duration::from_secs(1));
                    }
                }
            }
        }
    }

    /// Get the session ID and offset to resume a partially-completed upload. Pass the result to
    /// [`UploadSession::resume`] to create a new session and resume the upload from the
    /// `start_offset` in the return value.
    pub fn get_resume(&self) -> UploadResume {
        UploadResume {
            start_offset: self.inner.complete_up_to(),
            session_id: self.inner.session_id.clone(),
        }
    }

    fn upload_block_with_retry(
        client: &C,
        inner: &SessionInner,
        arg: &files::UploadSessionAppendArg,
        buf: &[u8],
        start_time: Instant,
        opts: &UploadOpts,
    ) -> Result<(), Error<UploadSessionAppendError>> {
        let block_start_time = Instant::now();
        let mut errors = 0;
        let mut backoff = opts.initial_backoff_time;
        loop {
            match files::upload_session_append_v2(client, arg, buf) {
                Ok(()) => {
                    break;
                }
                Err(Error::RateLimited {
                    reason,
                    retry_after_seconds,
                }) => {
                    warn!(
                        "rate-limited ({}), waiting {} seconds",
                        reason, retry_after_seconds
                    );
                    if retry_after_seconds > 0 {
                        sleep(Duration::from_secs(u64::from(retry_after_seconds)));
                    }
                }
                Err(e) => {
                    errors += 1;
                    if errors == opts.retry_count {
                        error!("Error calling upload_session_append: {e}, failing.");
                        return Err(e);
                    } else {
                        warn!("Error calling upload_session_append: {e}, retrying.");
                    }
                    sleep(jitter(backoff));
                    if backoff < opts.max_backoff_time {
                        backoff *= 2;
                    }
                }
            }
        }

        let now = Instant::now();
        let block_dur = now.duration_since(block_start_time);
        let overall_dur = now.duration_since(start_time);

        let block_bytes = buf.len() as u64;
        let bytes_sofar = inner.bytes_transferred.fetch_add(block_bytes, SeqCst) + block_bytes;

        // This assumes that we have `PARALLELISM` uploads going at the same time and at roughly the
        // same upload speed:
        let block_rate = block_bytes as f64 / block_dur.as_secs_f64() * opts.parallelism as f64;

        let overall_rate = bytes_sofar as f64 / overall_dur.as_secs_f64();

        if let Some(handler) = &opts.progress_handler {
            handler.update(bytes_sofar, block_rate, overall_rate);
        }

        Ok(())
    }
}

impl SessionInner {
    /// Generate the argument to append a block at the given offset.
    fn append_arg(&self, block_offset: u64) -> files::UploadSessionAppendArg {
        files::UploadSessionAppendArg::new(files::UploadSessionCursor::new(
            self.session_id.clone(),
            self.start_offset + block_offset,
        ))
    }

    /// Generate the argument to commit the upload at the given path with the given modification
    /// time.
    fn commit_arg(&self, commit_info: files::CommitInfo) -> files::UploadSessionFinishArg {
        files::UploadSessionFinishArg::new(
            files::UploadSessionCursor::new(
                self.session_id.clone(),
                self.bytes_transferred.load(SeqCst),
            ),
            commit_info,
        )
    }

    /// Mark a block as uploaded.
    fn mark_block_uploaded(&self, block_offset: u64, block_len: u64) {
        let mut completion = self.completion.lock().unwrap();
        completion.complete_block(self.start_offset + block_offset, block_len);
    }

    /// Return the offset up to which the file is completely uploaded. It can be resumed from this
    /// position if something goes wrong.
    fn complete_up_to(&self) -> u64 {
        let completion = self.completion.lock().unwrap();
        completion.complete_up_to
    }
}

/// Because blocks can be uploaded out of order, if an error is encountered when uploading a given
/// block, that is not necessarily the correct place to resume uploading from next time: there may
/// be gaps before that block.
///
/// This struct is for keeping track of what offset the file has been completely uploaded to.
///
/// When a block is finished uploading, call `complete_block` with the offset and length.
#[derive(Default)]
struct CompletionTracker {
    complete_up_to: u64,
    uploaded_blocks: HashMap<u64, u64>,
}

impl CompletionTracker {
    /// Make a new CompletionTracker that assumes everything up to the given offset is complete. Use
    /// this if resuming a previously interrupted session.
    pub fn resume_from(complete_up_to: u64) -> Self {
        Self {
            complete_up_to,
            uploaded_blocks: HashMap::new(),
        }
    }

    /// Mark a block as completely uploaded.
    pub fn complete_block(&mut self, block_offset: u64, block_len: u64) {
        if block_offset == self.complete_up_to {
            // Advance the cursor.
            self.complete_up_to += block_len;

            // Also look if we can advance it further still.
            while let Some(len) = self.uploaded_blocks.remove(&self.complete_up_to) {
                self.complete_up_to += len;
            }
        } else {
            // This block isn't at the low-water mark; there's a gap behind it. Save it for later.
            self.uploaded_blocks.insert(block_offset, block_len);
        }
    }
}

// Add a random duration in the range [-duration/4, duration/4].
fn jitter(duration: Duration) -> Duration {
    use ring::rand::{generate, SystemRandom};
    let rng = SystemRandom::new();
    let bytes: [u8; 4] = generate(&rng).unwrap().expose();
    let u = u32::from_ne_bytes(bytes);
    let max = f64::from(u32::MAX);
    let f = f64::from(u) / max / 4.;
    if u % 2 == 0 {
        duration + duration.mul_f64(f)
    } else {
        duration - duration.mul_f64(f)
    }
}
