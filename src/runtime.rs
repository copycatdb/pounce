use once_cell::sync::Lazy;
use tokio::runtime::Runtime;

pub static RUNTIME: Lazy<Runtime> = Lazy::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("Failed to create Tokio runtime")
});

pub fn block_on<F: std::future::Future>(future: F) -> F::Output {
    RUNTIME.block_on(future)
}
