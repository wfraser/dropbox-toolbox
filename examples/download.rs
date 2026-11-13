use dropbox_sdk::default_client::UserAuthDefaultClient;
use dropbox_sdk::files::{DownloadArg, DownloadError};
use dropbox_toolbox::RetryOpts;
use std::collections::VecDeque;
use std::sync::Arc;
use std::{env, io, process};

struct Args {
    path: String,
    verbose: bool,
}

fn parse_args() -> Args {
    let mut args = env::args().collect::<VecDeque<_>>();

    let mut dashdash = false;
    let mut verbose = false;
    let mut path = None;

    let program = args.pop_front().unwrap_or_else(|| "download".to_owned());

    while let Some(arg) = args.pop_front() {
        if !dashdash {
            if arg == "--" {
                dashdash = true;
                continue;
            }
            if arg == "-v" || arg == "--verbose" {
                verbose = true;
                continue;
            } else if arg == "-h" || arg == "--help" {
                path = None;
                break;
            } else if arg.starts_with("-") {
                eprintln!("unrecognized flag: {:?}", arg);
                path = None;
                break;
            }
        }

        if path.is_some() {
            eprintln!("too many arguments");
            path = None;
            break;
        }
        path = Some(arg);
    }

    let Some(path) = path else {
        eprintln!("usage: {program} [-v|--verbose] <dropbox-path>");
        process::exit(1);
    };

    Args { path, verbose }
}

enum Error {
    Dropbox(dropbox_sdk::Error<DownloadError>),
    LocalIo(io::Error),
}

fn main() {
    env_logger::init();

    let args = parse_args();

    let auth = dropbox_sdk::oauth2::get_auth_from_env_or_prompt();
    let client = Arc::new(UserAuthDefaultClient::new(auth));

    let arg = DownloadArg::new(args.path.clone());

    let mut stdout = io::stdout().lock();
    let result = dropbox_toolbox::download::download(client, RetryOpts::default(), arg, None, None)
        .map_err(Error::Dropbox)
        .and_then(|mut session| match io::copy(&mut session, &mut stdout) {
            Ok(n) => {
                if n != session.content_length() {
                    Err(Error::LocalIo(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "downloaded fewer bytes than content-length",
                    )))
                } else if n != session.bytes_read() {
                    Err(Error::LocalIo(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "copied fewer bytes than downloaded",
                    )))
                } else {
                    Ok(session)
                }
            }
            Err(e) => match e.downcast::<dropbox_sdk::Error<DownloadError>>() {
                Ok(e) => Err(Error::Dropbox(e)),
                Err(e) => Err(Error::LocalIo(e)),
            },
        });

    match result {
        Ok(session) => {
            if args.verbose {
                eprintln!(
                    "Download of {} succeeded: {:#?}",
                    args.path,
                    session.metadata()
                );
            }
        }
        Err(Error::Dropbox(e)) => {
            eprintln!("Dropbox error: {e:#}");
        }
        Err(Error::LocalIo(e)) => {
            eprintln!("Local I/O error: {e:#}");
        }
    }
}
