use arrow::array::Array;
use extendr_api::prelude::*;
use geo_index::rtree::RTreeIndex;
use rayon::prelude::*;

use super::metric::Metric;
use super::{as_geoms_and_rects, index_rects, rows_to_list_array};
use crate::threads::with_pool;
use crate::{check_recycle_len, try_float_array};

/// Rows of `y` within a distance of each row of `x`, validated in R
/// @noRd
#[extendr]
fn ga_sparse_dwithin_impl(
    x: Robj,
    y: Robj,
    distance: Robj,
    metric: &str,
) -> extendr_api::Result<Robj> {
    let metric = Metric::parse(metric)?;
    let threads = crate::threads::Threads::get();
    let (xs, x_rects) = as_geoms_and_rects(threads, x)?;
    let (ys, y_rects) = as_geoms_and_rects(threads, y)?;
    metric.check(&xs, "x")?;
    metric.check(&ys, "y")?;

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
        Some((tree, positions)) => with_pool(threads, || {
            (0..xs.len())
                .into_par_iter()
                .map(|i| {
                    let (Some(geom), Some(rect), Some(d)) = (&xs[i], &x_rects[i], radius(i)) else {
                        return None;
                    };
                    // nothing within `d` can sit outside the box grown to reach it
                    let (lon, lat) = metric.pad(d, rect);
                    let mut rows = tree
                        .search(
                            rect.min().x - lon,
                            rect.min().y - lat,
                            rect.max().x + lon,
                            rect.max().y + lat,
                        )
                        .into_iter()
                        .filter_map(|j| positions.get(j as usize).copied())
                        .filter(|row| {
                            ys[*row as usize].as_ref().is_some_and(|other| {
                                metric.distance(geom, other).is_some_and(|m| m <= d)
                            })
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
    fn ga_sparse_dwithin_impl;
}
