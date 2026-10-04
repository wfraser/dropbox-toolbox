use std::time::Duration;
use combine_structs::Fields;

#[allow(dead_code)]
#[derive(Fields)]
struct UploadDownloadCommonOpts {
    /// How many blocks (of [`BLOCK_SIZE`](crate::BLOCK_SIZE) bytes each) are uploaded in each
    /// request.
    ///
    /// Uploading multiple blocks per request reduces the number of requests needed to complete the
    /// upload and can reduce overhead and help avoid running into rate limits, at the cost of
    /// increasing the cost of a request that has to be retried in the event of an error.
    pub blocks_per_request: usize,

    /// How many consecutive errors until retries are abandoned and the upload is failed?
    pub retry_count: u32,

    /// Errors when uploading are handled with retry and exponential backoff with jitter. The first
    /// backoff will be this long, and subsequent backoffs will each be doubled in length (up to
    /// [`max_backoff_time`](crate::upload::UploadOpts::max_backoff_time)), until [`retry_count`](crate::upload::UploadOpts::retry_count)
    /// retries have been attempted, or the upload request succeeds.
    pub initial_backoff_time: Duration,

    /// Exponential backoff duration won't increase past this time.
    pub max_backoff_time: Duration,
}