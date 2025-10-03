use anyhow::Context;
use dropbox_sdk::files::{RelocationError, WriteConflictError, WriteError};
use std::error::Error;

#[test]
fn test_downcast_search() {
    fn some_api_call() -> Result<(), dropbox_sdk::Error<RelocationError>> {
        // Whew!
        Err(dropbox_sdk::Error::Api(RelocationError::FromWrite(
            WriteError::Conflict(WriteConflictError::File),
        )))
    }

    fn some_complex_fn_anyhow() -> anyhow::Result<()> {
        some_api_call().context("some api call failed")?;
        Ok(())
    }

    assert_eq!(
        Some(&WriteConflictError::File),
        some_complex_fn_anyhow()
            .unwrap_err()
            .chain()
            .find_map(<dyn Error>::downcast_ref)
    );

    fn some_complex_fn_box() -> Result<(), Box<dyn Error>> {
        some_api_call()?;
        Ok(())
    }

    // We can also implement anyhow::Error::chain() manually:
    struct ErrChain<'a>(Option<&'a (dyn Error + 'static)>);
    impl<'a> Iterator for ErrChain<'a> {
        type Item = &'a (dyn Error + 'static);
        fn next(&mut self) -> Option<Self::Item> {
            let next = self.0.take();
            if let Some(next) = next {
                self.0 = next.source();
            }
            next
        }
    }

    assert_eq!(
        Some(&WriteConflictError::File),
        some_complex_fn_box()
            .as_ref()
            .map_err(|e| ErrChain(Some(e.as_ref())))
            .unwrap_err()
            .find_map(<dyn Error>::downcast_ref)
    );
}
