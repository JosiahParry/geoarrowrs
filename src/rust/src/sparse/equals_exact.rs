use arrow::array::Array;
use extendr_api::prelude::*;
use geo_index::rtree::RTreeIndex;
use rayon::prelude::*;

use super::{as_geoms_and_rects, index_rects, rows_to_list_array};
use crate::threads::{Threads, with_pool};
use crate::topology::EqualsExact;
use crate::{check_recycle_len, try_float_array};

/// Find which rows of `y` are identical to each row of `x` within a tolerance
///
/// Returns the row numbers of `y` that have the same type and structure as
/// each row of `x`, with every coordinate within `tolerance` of its
/// counterpart. This is the sparse form of [ga_equals_exact()], and matches
/// `sf::st_equals_exact()`.
///
/// @details
/// `ga_sparse_equals_exact()` indexes `y` in a packed Hilbert R-tree and
/// compares only the rows whose bounding boxes come within `tolerance` of the
/// box of the row of `x`. It compares coordinates in order, so it does not
/// match a line to its reverse; use [ga_sparse_equals_topo()] for that.
///
/// `ga_sparse_equals_exact()` returns a zero length element for a row of `x`
/// that matches nothing. It returns a null element for a null or empty
/// geometry in `x`, and for a `tolerance` that is `NA` or negative.
///
/// @param x a GeoArrow geometry array
/// @param y a GeoArrow geometry array
/// @param tolerance the largest planar distance allowed between matching
///   coordinates; length 1 or the same length as `x`
/// @returns a list array of 1 based row numbers into `y`, the same length as `x`
/// @export
/// @family topology
/// @examplesIf requireNamespace("sf", quietly = TRUE) && requireNamespace("geoarrow", quietly = TRUE)
/// nc <- as.data.frame(read_shapefile(
///   system.file("shape/nc.shp", package = "sf")
/// ))
/// shifted <- ga_translate(nc$geometry, 0.001, 0)
///
/// # each county matches its shifted copy once the tolerance covers the shift
/// as.vector(ga_sparse_equals_exact(nc$geometry, shifted, 0))[1:3]
/// as.vector(ga_sparse_equals_exact(nc$geometry, shifted, 0.01))[1:3]
#[extendr]
fn ga_sparse_equals_exact(
    x: Robj,
    y: Robj,
    #[extendr(default = "0")] tolerance: Robj,
) -> extendr_api::Result<Robj> {
    let threads = Threads::get();
    let (xs, x_rects) = as_geoms_and_rects(threads, x)?;
    let (ys, y_rects) = as_geoms_and_rects(threads, y)?;

    let tolerances = try_float_array(tolerance, "tolerance")?;
    check_recycle_len(tolerances.len(), xs.len(), "tolerance")?;
    let tol = |i: usize| {
        let at = i % tolerances.len();
        if tolerances.is_null(at) {
            return None;
        }
        let t = tolerances.value(at);
        (t.is_finite() && t >= 0.0).then_some(t)
    };

    let found = match index_rects(&y_rects) {
        Some((tree, positions)) => with_pool(threads, || {
            (0..xs.len())
                .into_par_iter()
                .map(|i| {
                    let (Some(geom), Some(rect), Some(t)) = (&xs[i], &x_rects[i], tol(i)) else {
                        return None;
                    };
                    // a match has every coordinate within `t`, so its box can be no further away
                    let mut rows = tree
                        .search(
                            rect.min().x - t,
                            rect.min().y - t,
                            rect.max().x + t,
                            rect.max().y + t,
                        )
                        .into_iter()
                        .filter_map(|j| positions.get(j as usize).copied())
                        .filter(|row| {
                            ys[*row as usize]
                                .as_ref()
                                .is_some_and(|other| geom.equals_exact(other, t))
                        })
                        .map(|row| row + 1)
                        .collect::<Vec<_>>();
                    rows.sort_unstable();
                    Some(rows)
                })
                .collect::<Vec<_>>()
        }),
        // no row of `y` has a box, so nothing matches, but the shape still has to line up
        None => (0..xs.len())
            .map(|i| x_rects[i].as_ref().and(tol(i)).map(|_| Vec::new()))
            .collect(),
    };

    rows_to_list_array(found)
}

extendr_module! {
    mod equals_exact;
    fn ga_sparse_equals_exact;
}
