use std::future::Future;
use tokio::runtime::{Handle, Runtime};
use std::sync::OnceLock;

static RUNTIME: OnceLock<Runtime> = OnceLock::new();

/// Get or initialize the global Tokio runtime
pub fn get_runtime() -> &'static Runtime {
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .expect("Failed to create Tokio runtime")
    })
}


/// Block on a future with borrowed data using the current runtime
pub fn block_on_borrowed<F, R>(future: F) -> R
where
    F: Future<Output = R>,
{
    match Handle::try_current() {
        Ok(handle) => {
            // We're already in a Tokio runtime context
            tokio::task::block_in_place(|| {
                handle.block_on(future)
            })
        }
        Err(_) => {
            // Not in a runtime, use our global one
            get_runtime().block_on(future)
        }
    }
}
