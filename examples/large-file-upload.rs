#![deny(rust_2018_idioms)]

//! This example illustrates advanced usage of Dropbox's chunked file upload API to upload large
//! files that would not fit in a single HTTP request, including allowing the user to resume
//! interrupted uploads, and uploading blocks in parallel.

use dropbox_sdk::default_client::UserAuthDefaultClient;
use dropbox_sdk::files;
use dropbox_toolbox::upload::{ProgressHandler, UploadOpts, UploadResume, UploadSession};
use std::fs::File;
use std::io::{Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::process::exit;
use std::sync::Arc;
use std::time::SystemTime;

macro_rules! fatal {
    ($($arg:tt)*) => {{
        eprintln!($($arg)*);
        exit(2);
    }}
}

fn usage() {
    eprintln!(
        "usage: {} <source file path> <Dropbox path> [--resume <session ID>,<resume offset>]",
        std::env::args().next().unwrap()
    );
}

enum Operation {
    Usage,
    Upload(Args),
}

#[derive(Debug)]
struct Args {
    source_path: PathBuf,
    dest_path: String,
    resume: Option<Resume>,
}

#[derive(Debug, Clone)]
struct Resume(UploadResume);

impl std::str::FromStr for Resume {
    type Err = &'static str;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut parts = s.rsplitn(2, ',');
        let offset_str = parts.next().ok_or("missing session ID and file offset")?;
        let session_id = parts.next().ok_or("missing file offset")?.to_owned();
        let start_offset = offset_str.parse().map_err(|_| "invalid file offset")?;
        Ok(Self(UploadResume {
            start_offset,
            session_id,
        }))
    }
}

fn parse_args() -> Operation {
    let mut a = std::env::args().skip(1);
    match (a.next(), a.next()) {
        (Some(ref arg), _) if arg == "--help" || arg == "-h" => Operation::Usage,
        (Some(src), Some(dest)) => {
            let resume = match (a.next().as_deref(), a.next()) {
                (Some("--resume"), Some(resume_str)) => match resume_str.parse() {
                    Ok(resume) => Some(resume),
                    Err(e) => {
                        eprintln!("Invalid --resume argument: {}", e);
                        return Operation::Usage;
                    }
                },
                (None, _) => None,
                _ => {
                    return Operation::Usage;
                }
            };
            Operation::Upload(Args {
                source_path: PathBuf::from(src),
                dest_path: dest,
                resume,
            })
        }
        (Some(_), None) => {
            eprintln!("missing destination path");
            Operation::Usage
        }
        (None, _) => Operation::Usage,
    }
}

/// Figure out if destination is a folder or not and change the destination path accordingly.
fn get_destination_path(
    client: &UserAuthDefaultClient,
    given_path: &str,
    source_path: &Path,
) -> Result<String, String> {
    let filename = source_path
        .file_name()
        .ok_or_else(|| format!("invalid source path {:?} has no filename", source_path))?
        .to_string_lossy();

    // Special-case: we can't get metadata for the root, so just use the source path filename.
    if given_path == "/" {
        let mut path = "/".to_owned();
        path.push_str(&filename);
        return Ok(path);
    }

    let meta_result =
        files::get_metadata(client, &files::GetMetadataArg::new(given_path.to_owned()));

    match meta_result {
        Ok(files::Metadata::File(_)) => {
            // We're not going to allow overwriting existing files.
            Err(format!("Path {} already exists in Dropbox", given_path))
        }
        Ok(files::Metadata::Folder(_)) => {
            // Given destination path points to a folder, so append the source path's filename and
            // use that as the actual destination.

            let mut path = given_path.to_owned();
            path.push('/');
            path.push_str(&filename);

            Ok(path)
        }
        Ok(files::Metadata::Deleted(_)) => panic!("unexpected deleted metadata received"),
        Err(dropbox_sdk::Error::Api(files::GetMetadataError::Path(
            files::LookupError::NotFound,
        ))) => {
            // Given destination path doesn't exist, which is just fine. Use the given path as-is.
            // Note that it's fine if the path's parents don't exist either; folders will be
            // automatically created as needed.
            Ok(given_path.to_owned())
        }
        Err(e) => Err(format!("Error looking up destination: {}", e)),
    }
}

fn get_file_mtime_and_size(f: &File) -> Result<(SystemTime, u64), String> {
    let meta = f
        .metadata()
        .map_err(|e| format!("Error getting source file metadata: {}", e))?;
    let mtime = meta
        .modified()
        .map_err(|e| format!("Error getting source file mtime: {}", e))?;
    Ok((mtime, meta.len()))
}

fn human_number(n: u64) -> String {
    let mut f = n as f64;
    let prefixes = ['k', 'M', 'G', 'T', 'P', 'E'];
    let mut mag = 0;
    while mag < prefixes.len() {
        if f < 1000. {
            break;
        }
        f /= 1000.;
        mag += 1;
    }
    if mag == 0 {
        format!("{} ", n)
    } else {
        format!("{:.02} {}", f, prefixes[mag - 1])
    }
}

fn iso8601(t: SystemTime) -> String {
    let timestamp: i64 = match t.duration_since(SystemTime::UNIX_EPOCH) {
        Ok(duration) => duration.as_secs() as i64,
        Err(e) => -(e.duration().as_secs() as i64),
    };

    chrono::DateTime::from_timestamp(timestamp, 0 /* nsecs */)
        .expect("invalid or out-of-range timestamp")
        .format("%Y-%m-%dT%H:%M:%SZ")
        .to_string()
}

struct Progress {
    source_len: u64,
    start_offset: u64,
}

impl ProgressHandler for Progress {
    fn update(&self, bytes_uploaded: u64, instant_rate: f64, overall_rate: f64) {
        let percent = (self.start_offset + bytes_uploaded) as f64 / self.source_len as f64 * 100.;

        eprintln!(
            "{:.01}%: {}Bytes uploaded, {}Bytes per second, {}Bytes per second average",
            percent,
            human_number(bytes_uploaded),
            human_number(instant_rate as u64),
            human_number(overall_rate as u64),
        );
    }
}

fn main() {
    env_logger::init();

    let args = match parse_args() {
        Operation::Usage => {
            usage();
            exit(1);
        }
        Operation::Upload(args) => args,
    };

    let mut source_file = File::open(&args.source_path).unwrap_or_else(|e| {
        fatal!("Source file {:?} not found: {}", args.source_path, e);
    });

    let auth = dropbox_sdk::oauth2::get_auth_from_env_or_prompt();
    let client = Arc::new(UserAuthDefaultClient::new(auth));

    let dest_path = get_destination_path(client.as_ref(), &args.dest_path, &args.source_path)
        .unwrap_or_else(|e| {
            fatal!("Error: {}", e);
        });

    eprintln!("source = {:?}", args.source_path);
    eprintln!("dest   = {:?}", dest_path);

    let (source_mtime, source_len) = get_file_mtime_and_size(&source_file)
        .unwrap_or_else(|e| fatal!("failed to get file mtime and size: {}", e));

    let session = if let Some(Resume(ref resume)) = args.resume {
        source_file
            .seek(SeekFrom::Start(resume.start_offset))
            .unwrap_or_else(|e| fatal!("Seek error: {}", e));
        UploadSession::resume(client, resume.clone())
    } else {
        UploadSession::new(client)
            .unwrap_or_else(|e| fatal!("failed to create upload session: {}", e))
    };

    let result = session
        .upload(
            source_file,
            UploadOpts {
                progress_handler: Some(Arc::new(Box::new(Progress {
                    source_len,
                    start_offset: args.resume.map(|r| r.0.start_offset).unwrap_or(0),
                }))),
                ..Default::default()
            },
        )
        .and_then(|bytes| {
            eprintln!("uploaded {} bytes.", bytes);
            session
                .commit(
                    files::CommitInfo::new(dest_path).with_client_modified(iso8601(source_mtime)),
                )
                .map_err(|e| e.boxed())
        })
        .unwrap_or_else(|_| {
            let resume = session.get_resume();
            fatal!(
                "Upload failed. To retry, use --resume {},{}",
                resume.session_id,
                resume.start_offset
            );
        });
    println!("{result:#?}");
}
