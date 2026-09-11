use arrow::array::Array;
use extendr_api::prelude::*;
use geo::{Distance, Euclidean};
use geo_index::rtree::RTreeIndex;
use rayon::prelude::*;

use super::{as_geoms_and_rects, index_rects, rows_to_list_array};
use crate::threads::with_pool;
use crate::{check_recycle_len, try_float_array};

/// Find which rows of `y` lie within a distance of each row of `x`
///
/// Returns the row numbers of `y` whose geometry is no further than `distance`
/// from each row of `x`. This is the sparse form of a distance band join, and
/// the same test as PostGIS `ST_DWithin()`.
///
/// @details
/// Distance is Euclidean and measured between the geometries themselves, so a
/// point counts when it is within `distance` of the nearest edge of a polygon,
/// not of the box around it. This is why it differs from buffering `x` and
/// intersecting: a buffer approximates its curves with segments, so a point
/// just inside the true radius can fall outside the buffer.
///
/// The bounding box of each row of `x` is grown by `distance` before the tree
/// is searched, so nothing within reach is missed and only the rows that could
/// qualify are measured. `distance` is recycled, so one value covers every row
/// or a different radius can apply to each.
///
/// A row that matches nothing gives a zero length element, not a null. A null
/// or empty geometry in `x`, or a null `distance`, gives a null element, and a
/// null geometry in `y` is never returned.
///
/// @param x a GeoArrow geometry array
/// @param y a GeoArrow geometry array
/// @param distance the furthest a row of `y` may be; length 1 or the same
///   length as `x`
/// @returns a list array of 1 based row numbers into `y`, the same length as `x`
/// @export
/// @family index
/// @examplesIf requireNamespace("sf", quietly = TRUE) && requireNamespace("geoarrow", quietly = TRUE)
/// nc <- as.data.frame(read_shapefile(
///   system.file("shape/nc.shp", package = "sf")
/// ))
/// sites <- ga_xy(c(-78.6, -80.8), c(35.8, 35.2))
///
/// # the counties within a quarter degree of each site
/// as.vector(ga_sparse_dwithin(sites, nc$geometry, 0.25))
#[extendr]
fn ga_sparse_dwithin(x: Robj, y: Robj, distance: Robj) -> extendr_api::Result<Robj> {
    let (xs, x_rects) = as_geoms_and_rects(x)?;
    let (ys, y_rects) = as_geoms_and_rects(y)?;

    let distances = try_float_array(distance, "distance")?;
    check_recycle_len(distances.len(), xs.len(), "distance")?;

    let radius = |i: usize| {
        let at = i % distances.len();
        if distances.is_null(at) {
            return None;
        }
        let d = distances.value(at);
        (d.is_finite() && d >= 0.0).then_some(d)
    };

    let found = match index_rects(&y_rects) {
        Some((tree, positions)) => with_pool(|| {
            (0..xs.len())
                .into_par_iter()
                .map(|i| {
                    let (Some(geom), Some(rect), Some(d)) = (&xs[i], &x_rects[i], radius(i)) else {
                        return None;
                    };
                    // nothing within `d` can sit outside the box grown by `d`
                    let mut rows = tree
                        .search(
                            rect.min().x - d,
                            rect.min().y - d,
                            rect.max().x + d,
                            rect.max().y + d,
                        )
                        .into_iter()
                        .filter_map(|j| positions.get(j as usize).copied())
                        .filter(|row| {
                            ys[*row as usize]
                                .as_ref()
                                .is_some_and(|other| Euclidean.distance(geom, other) <= d)
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
            .map(|i| x_rects[i].as_ref().and(radius(i)).map(|_| Vec::new()))
            .collect(),
    };

    rows_to_list_array(found)
}

extendr_module! {
    mod dwithin;
    fn ga_sparse_dwithin;
}
