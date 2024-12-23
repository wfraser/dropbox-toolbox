//! Functions for listing directories.

use std::collections::VecDeque;
use std::thread::sleep;
use std::time::Duration;

use dropbox_sdk::Error;
use dropbox_sdk::files::{ListFolderError, ListFolderContinueError};
use dropbox_sdk::{files, UserAuthClient};

/// Make an iterator that yields directory entries under a given path, optionally recursively.
pub fn list_directory<'a, T: UserAuthClient>(
    client: &'a T,
    path: &str,
    recursive: bool,
) -> Result<DirectoryIterator<'a, T>, Error<ListFolderError>> {
    assert!(
        path.starts_with('/'),
        "path needs to be absolute (start with a '/')"
    );
    let requested_path = if path == "/" {
        // Root folder should be requested as empty string.
        String::new()
    } else {
        path.to_owned()
    };
    let result = list_folder_internal(
        client,
        files::list_folder,
        &files::ListFolderArg::new(requested_path).with_recursive(recursive),
    )?;
    let cursor = if result.has_more {
        Some(result.cursor)
    } else {
        None
    };
    Ok(DirectoryIterator {
        client,
        cursor,
        buffer: result.entries.into(),
    })
}

/// An iterator over directory entries which pages though the Dropbox API as necessary.
pub struct DirectoryIterator<'a, T: UserAuthClient> {
    client: &'a T,
    buffer: VecDeque<files::Metadata>,
    cursor: Option<String>,
}

impl<T: UserAuthClient> Iterator for DirectoryIterator<'_, T> {
    type Item = Result<files::Metadata, Error<ListFolderContinueError>>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(entry) = self.buffer.pop_front() {
            Some(Ok(entry))
        } else if let Some(cursor) = self.cursor.take() {
            let result = match list_folder_internal(
                self.client,
                files::list_folder_continue,
                &files::ListFolderContinueArg::new(cursor),
            ) {
                Ok(r) => r,
                Err(e) => return Some(Err(e)),
            };
            self.buffer.extend(result.entries);
            if result.has_more {
                self.cursor = Some(result.cursor);
            }
            self.buffer.pop_front().map(Ok)
        } else {
            None
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (
            self.buffer.len(),
            if self.cursor.is_none() {
                Some(self.buffer.len())
            } else {
                None
            },
        )
    }
}

fn list_folder_internal<T, A, E>(
    client: &T,
    f: impl Fn(&T, &A) -> Result<files::ListFolderResult, Error<E>>,
    arg: &A,
) -> Result<files::ListFolderResult, Error<E>>
where
    T: UserAuthClient,
    A: Clone,
    E: std::error::Error + Send + Sync + 'static,
{
    let mut errors = 0;
    loop {
        match f(client, arg) {
            Ok(r) => break Ok(r),
            Err(Error::RateLimited {
                reason,
                retry_after_seconds,
            }) => {
                warn!("rate-limited ({reason}), waiting {retry_after_seconds} seconds");
                if retry_after_seconds > 0 {
                    sleep(Duration::from_secs(u64::from(retry_after_seconds)));
                }
            }
            Err(e) => {
                errors += 1;
                if errors == 3 {
                    warn!("Error calling list_folder_continue: {e}, failing");
                    return Err(e);
                } else {
                    warn!("Error calling list_folder_continue: {e}, retrying.");
                }
            }
        }
    }
}
