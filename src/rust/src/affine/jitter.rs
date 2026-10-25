use arrow::array::Float64Array;
use extendr_api::prelude::*;
use rand::{RngExt, SeedableRng};
use rand_pcg::Pcg64;

use super::translate::translate_by;
use crate::envelope::{as_rects, extent_of};
use crate::threads::Threads;
use crate::{as_geometry_chunks, check_recycle_len, try_float_array};

/// Move each geometry by a small random offset
///
/// Shifts each geometry of `x` as a whole by a random distance along x and
/// along y, each drawn uniformly between `-amount` and `amount`. This matches
/// `sf::st_jitter()` in everything but the random numbers.
///
/// @details
/// Without `amount`, `ga_jitter()` uses `factor` times the length of the
/// diagonal of the box around all of `x`, as sf does. With `longlat = TRUE`,
/// it narrows the x offsets by the cosine of the mean latitude of that box, as
/// sf does for geographic coordinates, so the jitter covers about the same
/// ground in both directions.
///
/// `ga_jitter()` draws the offsets from its own generator, seeded by `seed`,
/// two for every row in order. The default seed comes from R's generator, so
/// `set.seed()` makes the result repeatable, but `ga_jitter()` never draws the
/// numbers sf would.
///
/// `ga_jitter()` keeps the geometry type of `x`. It returns null for a null
/// geometry, and for an `amount` that is `NA`, negative, or infinite.
///
/// @param x a GeoArrow geometry array
/// @param amount the most to move a geometry along each axis; length 1 or the
///   same length as `x`, or `NULL` to take it from `factor`
/// @param factor the fraction of the diagonal of the box around `x` to move a
///   geometry by at most when you give no `amount`
/// @param longlat whether `x` holds longitude and latitude
/// @param seed a whole number that seeds the offsets
/// @returns a GeoArrow array of the same geometry type and length as `x`
/// @export
/// @family affine
/// @examplesIf requireNamespace("geoarrow", quietly = TRUE)
/// pts <- ga_point(c(0, 10, 10), c(0, 0, 10))
///
/// # each point moves by at most 0.5 along each axis
/// moved <- ga_jitter(pts, amount = 0.5, seed = 1)
/// as.vector(ga_x(moved)) - c(0, 10, 10)
///
/// # the same seed moves each point the same way
/// identical(
///   as.vector(ga_x(ga_jitter(pts, seed = 42))),
///   as.vector(ga_x(ga_jitter(pts, seed = 42)))
/// )
#[extendr]
fn ga_jitter(
    x: Robj,
    #[extendr(default = "NULL")] amount: Robj,
    #[extendr(default = "0.002")] factor: f64,
    #[extendr(default = "FALSE")] longlat: bool,
    #[extendr(default = "sample.int(.Machine$integer.max, 1L)")] seed: u64,
) -> extendr_api::Result<Robj> {
    let chunks = as_geometry_chunks(x.clone())?;
    let n = chunks.iter().map(|c| c.len()).sum::<usize>();

    // the box around every row sizes the default amount and the longlat correction
    let extent = if amount.is_null() || longlat {
        extent_of(&as_rects(Threads::get(), &chunks)?)
    } else {
        None
    };

    let amounts = if amount.is_null() {
        // an array with no coordinates has nothing to move
        let diagonal = extent.map_or(0.0, |r| r.width().hypot(r.height()));
        Float64Array::from(vec![factor * diagonal])
    } else {
        try_float_array(amount, "amount")?
    };
    check_recycle_len(amounts.len(), n, "amount")?;

    let x_scale = match extent {
        Some(r) if longlat => r.center().y.to_radians().cos(),
        _ => 1.0,
    };

    let mut rng = Pcg64::seed_from_u64(seed);
    let (xs, ys) = amounts
        .iter()
        .cycle()
        .take(n)
        .map(|a| {
            // every row draws its pair, so a row's offsets depend only on the seed and its position
            let (u, v) = (rng.random::<f64>(), rng.random::<f64>());
            match a.filter(|a| a.is_finite() && *a >= 0.0) {
                Some(a) => (
                    Some((2.0 * u - 1.0) * a * x_scale),
                    Some((2.0 * v - 1.0) * a),
                ),
                None => (None, None),
            }
        })
        .unzip::<_, _, Vec<_>, Vec<_>>();

    translate_by(x, &Float64Array::from(xs), &Float64Array::from(ys))
}

extendr_module! {
    mod jitter;
    fn ga_jitter;
}
