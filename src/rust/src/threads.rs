use extendr_api::prelude::*;
use rayon::{ThreadPool, ThreadPoolBuilder};
use std::sync::{Arc, Mutex, OnceLock};

/// The thread count last asked for, alongside the pool built for it.
type PoolCache = Mutex<Option<(usize, Arc<ThreadPool>)>>;

/// The pool built for the last thread count asked for, so that a repeated call does not rebuild it.
fn cached() -> &'static PoolCache {
    static CACHE: OnceLock<PoolCache> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(None))
}

/// Read `geoarrowrs.thread_pool`, which caps how many threads the parallel paths use.
fn requested() -> Option<usize> {
    let opt = R!("getOption('geoarrowrs.thread_pool')").ok()?;
    let n = opt
        .as_integer()
        .or_else(|| opt.as_real().map(|v| v as i32))?;
    if n > 0 { Some(n as usize) } else { None }
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

/// Run a parallel closure on the pool the user asked for, or on rayon's own if they asked for nothing.
///
/// Reading the option per call keeps `options()` live, and costs nothing next
/// to the work the closure does.
pub(crate) fn with_pool<F, T>(f: F) -> T
where
    F: FnOnce() -> T + Send,
    T: Send,
{
    match requested().and_then(pool_for) {
        Some(pool) => pool.install(f),
        None => f(),
    }
}
