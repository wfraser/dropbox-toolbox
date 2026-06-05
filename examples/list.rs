//! This example uses dropbox_toolbox's [`list_directory()`] helper, which takes care of paging
//! through Dropbox SDK `list_folder` and `list_folder_continue` results.

use dropbox_sdk::default_client::UserAuthDefaultClient;
use dropbox_sdk::files::Metadata;
use dropbox_toolbox::list::list_directory;
use std::env;
use std::process::exit;
use std::sync::Arc;

fn main() {
    env_logger::init();

    let mut args = env::args().skip(1);
    let path = match (args.next(), args.next()) {
        (Some(path), None) => path,
        _ => {
            eprintln!("usage: list <path>");
            exit(1);
        }
    };

    let auth = dropbox_sdk::oauth2::get_auth_from_env_or_prompt();
    let client = Arc::new(UserAuthDefaultClient::new(auth));

    let iter = list_directory(client.as_ref(), &path, true)
        .unwrap_or_else(|e| {
            eprintln!("Failed to list directory {path:?}: {e:#}");
            exit(2);
        });

    for result in iter {
        let item = result.unwrap_or_else(|e| {
            eprintln!("Error fetching directory entries: {e:#}");
            exit(3);
        });

        let path_display = match &item {
            Metadata::File(f) => &f.path_display,
            Metadata::Folder(f) => &f.path_display,
            Metadata::Deleted(d) => {
                // These should only be present when listing from a cursor, which we are not.
                println!("! unexpected delete entry: {d:?}");
                continue;
            }
        };

        let Some(path_display) = path_display else {
            // It's an Option<String>, but in practice it is required to be there and always is.
            println!("! missing path_display: {item:?}");
            continue;
        };

        if matches!(item, Metadata::Folder(_)) {
            println!("{path_display}/");
        } else {
            println!("{path_display}");
        }
    }
}
