//! Functions for downloading files.

use std::time::Duration;
use combine_structs::combine_fields;

/// Options for how to perform downloads.
#[combine_fields(UploadDownloadCommonOpts)]
#[derive(Clone)]
pub struct DownloadOpts {}

