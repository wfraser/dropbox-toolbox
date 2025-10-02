use dropbox_sdk::dbx_async::PollArg;
use dropbox_sdk::default_client::{NoauthDefaultClient, UserAuthDefaultClient};
use dropbox_sdk::files::{
    move_batch_check_v2, move_batch_v2, Metadata, MoveBatchArg, RelocationBatchResultEntry,
    RelocationBatchV2JobStatus, RelocationBatchV2Launch, RelocationPath,
};
use dropbox_sdk::oauth2::get_auth_from_env_or_prompt;
use dropbox_sdk::BoxedError;
use dropbox_toolbox::list::list_directory;
use dropbox_toolbox::ResultExt;
use std::io::{stdin, stdout, Write};
use std::time::Duration;
use std::{env, thread};

fn main() -> Result<(), BoxedError> {
    env_logger::init();

    let path = env::args().nth(1).unwrap_or_else(|| {
        panic!("usage: program <starting path>");
    });

    let mut auth = get_auth_from_env_or_prompt();
    if auth.save().is_none() {
        auth.obtain_access_token(NoauthDefaultClient::default())
            .boxed_err()?;
    }
    println!("auth: {:?}", auth.save());
    let client = UserAuthDefaultClient::new(auth);

    let mut batch = vec![];
    for result in list_directory(&client, &path, true).boxed_err()? {
        let Metadata::File(entry) = result.boxed_err()? else {
            continue;
        };
        const BAD_CHARS: [char; 8] = ['\\', '"', '<', '>', ':', '|', '?', '*'];
        if entry.name.contains(BAD_CHARS) {
            let path = entry.path_display.unwrap();
            let fixed = path.replace(BAD_CHARS, "_");
            batch.push(RelocationPath::new(path, fixed));
        }
    }

    println!("have these renames to do: {batch:#?}");
    print!("ok? [y/n] ");
    stdout().flush().unwrap();
    let mut input = String::new();
    stdin().read_line(&mut input).unwrap();
    if input.trim() != "y" {
        println!("aborting");
        return Ok(());
    }

    let result = match move_batch_v2(&client, &MoveBatchArg::new(batch)).boxed_err()? {
        RelocationBatchV2Launch::AsyncJobId(id) => loop {
            match move_batch_check_v2(&client, &PollArg::new(id.clone())).boxed_err()? {
                RelocationBatchV2JobStatus::InProgress => {
                    thread::sleep(Duration::from_secs(1));
                }
                RelocationBatchV2JobStatus::Complete(done) => {
                    break done;
                }
            }
        },
        RelocationBatchV2Launch::Complete(done) => done,
    };

    for entry in result.entries {
        match entry {
            RelocationBatchResultEntry::Success(_) => {}
            RelocationBatchResultEntry::Failure(e) => {
                println!("entry failed: {e:?}");
            }
            RelocationBatchResultEntry::Other | _ => {}
        }
    }

    Ok(())
}
