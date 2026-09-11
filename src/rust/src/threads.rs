use extendr_api::prelude::*;
use rayon::{ThreadPool, ThreadPoolBuilder};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

/// The thread cap R last set, or 0 for rayon's own default.
///
/// An atomic rather than a lookup, because the parallel paths must never
/// evaluate R. Some of these functions are registered as Acero kernels, so a
/// call can arrive on one of Arrow's worker threads rather than on R's own, and
/// touching the R API from there is undefined behaviour whatever it returns.
static THREADS: AtomicUsize = AtomicUsize::new(0);

/// How many threads a parallel path may use, carried rather than looked up.
#[derive(Clone, Copy, Default)]
pub(crate) struct Threads(Option<usize>);

impl Threads {
    /// What R last asked for.
    pub(crate) fn get() -> Self {
        match THREADS.load(Ordering::Relaxed) {
            0 => Self(None),
            n => Self(Some(n)),
        }
    }
}

/// Set the thread cap, from `options(geoarrowrs.thread_pool = )`
///
/// Called on load and whenever the option changes. `0` returns the parallel
/// paths to rayon's own default, which is one thread per core.
///
/// @param n the most threads to use, or `0` for rayon's default
/// @returns `n`, invisibly
/// @export
/// @family index
/// @examples
/// # hold the parallel paths to two threads
/// ga_set_thread_pool(2)
///
/// # and back to one per core
/// ga_set_thread_pool(0)
#[extendr]
fn ga_set_thread_pool(n: i32) -> extendr_api::Result<i32> {
    if n < 0 {
        return Err(Error::Other("`n` must not be negative".to_string()));
    }
    THREADS.store(n as usize, Ordering::Relaxed);
    Ok(n)
}

/// The thread count last asked for, alongside the pool built for it.
type PoolCache = Mutex<Option<(usize, Arc<ThreadPool>)>>;

/// The pool built for the last thread count asked for, so that a repeated call does not rebuild it.
fn cached() -> &'static PoolCache {
    static CACHE: OnceLock<PoolCache> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(None))
}

/// Get, building if needed, the pool for a given thread count.
fn pool_for(n: usize) -> Option<Arc<ThreadPool>> {
    let mut guard = cached().lock().ok()?;
    if let Some((have, pool)) = guard.as_ref()
        && *have == n
    {
        return Some(pool.clone());
    }

    let pool = Arc::new(ThreadPoolBuilder::new().num_threads(n).build().ok()?);
    *guard = Some((n, pool.clone()));
    Some(pool)
}

/// Run a parallel closure on the pool asked for, or on rayon's own if none was.
pub(crate) fn with_pool<F, T>(threads: Threads, f: F) -> T
where
    F: FnOnce() -> T + Send,
    T: Send,
{
    match threads.0.and_then(pool_for) {
        Some(pool) => pool.install(f),
        None => f(),
    }
}

extendr_module! {
    mod threads;
    fn ga_set_thread_pool;
}
