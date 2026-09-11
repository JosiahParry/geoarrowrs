use arrow::array::{Array, Float64Array, ListArray};
use arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow::datatypes::{DataType, Field};
use arrow_extendr::IntoArrowRobj;
use extendr_api::prelude::*;
use geo::{Distance, Euclidean, Geodesic, Haversine, Rhumb, VincentyDistance};
use rayon::prelude::*;
use std::sync::Arc;

use crate::threads::with_pool;
use crate::{as_geo_geometries_par, as_geometry_chunks, as_points};

/// Every distance from each row of `xs` to all of `ys`, one rayon task per row.
///
/// The result is rectangular, so the values go straight into one flat buffer
/// rather than through a builder: each row owns the `ys.len()` slots at its own
/// offset and nothing has to be appended in order afterwards.
fn cross<T, U>(
    xs: &[Option<T>],
    ys: &[Option<U>],
    metric: impl Fn(&T, &U) -> Option<f64> + Sync,
) -> extendr_api::Result<Robj>
where
    T: Sync,
    U: Sync,
{
    let width = ys.len();
    let total = xs
        .len()
        .checked_mul(width)
        .ok_or_else(|| Error::Other("too many pairs to hold at once".into()))?;

    let mut values = vec![0.0; total];
    let mut valid = vec![false; total];

    if width > 0 {
        with_pool(|| {
            values
                .par_chunks_mut(width)
                .zip(valid.par_chunks_mut(width))
                .zip(xs.par_iter())
                .for_each(|((row, row_valid), x)| {
                    let Some(x) = x else { return };
                    for (j, y) in ys.iter().enumerate() {
                        if let Some(d) = y.as_ref().and_then(|y| metric(x, y)) {
                            row[j] = d;
                            row_valid[j] = true;
                        }
                    }
                });
        });
    }

    let distances = Float64Array::new(ScalarBuffer::from(values), Some(NullBuffer::from(valid)));
    let rows = NullBuffer::from_iter(xs.iter().map(|x| x.is_some()));

    ListArray::try_new(
        Arc::new(Field::new_list_field(DataType::Float64, true)),
        OffsetBuffer::from_lengths(std::iter::repeat_n(width, xs.len())),
        Arc::new(distances),
        Some(rows),
    )
    .map_err(|e| Error::Other(e.to_string()))?
    .into_data()
    .into_arrow_robj()
}

/// Distance from every row of `x` to every row of `y`
///
/// Measures the full cross product rather than walking the two arrays in
/// lockstep, giving one list of `length(y)` distances per row of `x`.
///
/// @details
/// This is the shape [ga_dist_euclidean_pairwise()] cannot express, and it
/// costs `length(x) * length(y)` distances to hold: ten thousand rows against
/// ten thousand is a hundred million doubles, or eight hundred megabytes. Where
/// the distances are only wanted to pick a nearest row or a threshold,
/// [ga_sparse_knn()] and [ga_sparse_dwithin()] answer that against an R-tree
/// without ever forming the product.
///
/// `"euclidean"` measures between geometries of any type. The spherical and
/// ellipsoidal metrics take points only, because `geo` defines them between
/// points alone. `"vincenty"` gives a null where the algorithm fails to
/// converge.
///
/// A null row of `x` gives a null element. A null row of `y` gives a null in
/// that position of every element.
///
/// @param x a GeoArrow geometry array for `metric = "euclidean"`, a point array
///   for the others
/// @param y a GeoArrow array matching `x`
/// @param metric one of `"euclidean"`, `"haversine"`, `"geodesic"`, `"rhumb"`,
///   or `"vincenty"`
/// @returns a list array with one element per row of `x`, each holding
///   `length(y)` distances
/// @export
/// @family distance
/// @examplesIf requireNamespace("geoarrow", quietly = TRUE)
/// x <- ga_xy(c(-78.6382, -80.8431), c(35.7796, 35.2271))
/// y <- ga_xy(c(-77.9447, -78.6382, -80.8431), c(34.2257, 35.7796, 35.2271))
///
/// # two elements of three distances each, in meters
/// ga_cross_distance(x, y, "haversine")
#[extendr]
fn ga_cross_distance(
    x: Robj,
    y: Robj,
    #[extendr(default = "\"euclidean\"")] metric: &str,
) -> extendr_api::Result<Robj> {
    if metric == "euclidean" {
        let xs = as_geo_geometries_par(&as_geometry_chunks(x)?)?;
        let ys = as_geo_geometries_par(&as_geometry_chunks(y)?)?;
        return cross(&xs, &ys, |a, b| Some(Euclidean.distance(a, b)));
    }

    let xs = as_points(x)?;
    let ys = as_points(y)?;

    match metric {
        "haversine" => cross(&xs, &ys, |a, b| Some(Haversine.distance(*a, *b))),
        "geodesic" => cross(&xs, &ys, |a, b| Some(Geodesic.distance(*a, *b))),
        "rhumb" => cross(&xs, &ys, |a, b| Some(Rhumb.distance(*a, *b))),
        "vincenty" => cross(&xs, &ys, |a, b| a.vincenty_distance(b).ok()),
        other => Err(Error::Other(format!(
            "`metric` must be one of \"euclidean\", \"haversine\", \"geodesic\", \"rhumb\", or \"vincenty\", not {other:?}"
        ))),
    }
}

extendr_module! {
    mod cross;
    fn ga_cross_distance;
}
