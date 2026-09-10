use arrow::array::Float64Builder;
use arrow_extendr::IntoArrowRobj;
use extendr_api::prelude::*;
use geo::{
    Distance, Euclidean, Geodesic, HausdorffDistance, Haversine, Rhumb, VincentyDistance,
    line_measures::FrechetDistance,
};
use geo_traits::to_geo::{ToGeoLineString, ToGeoPoint};
use geoarrow::array::{GeoArrowArray, GeoArrowArrayAccessor, LineStringArray, PointArray};

use crate::{
    as_geo_geometries, as_geometry_chunks, as_linestring_chunks, as_point_chunks, check_pair_len,
};

/// Compute pairwise distances between points
///
/// Calculate the distance between each pair of points in `origin` and `dest`.
/// These functions differ in the metric space used for the calculation.
///
/// @param origin a GeoArrow point array of origin points
/// @param dest a GeoArrow point array of destination points
/// @returns a double vector of distance values
/// @export
/// @rdname dist_pairwise
/// @family distance
/// @references
///   [Distance](https://docs.rs/geo/latest/geo/algorithm/line_measures/trait.Distance.html),
///   [Euclidean](https://docs.rs/geo/latest/geo/algorithm/line_measures/metric_spaces/struct.Euclidean.html)
// TODO: use rayon with min chunk size of 4096
#[extendr]
fn ga_dist_euclidean_pairwise(origin: Robj, dest: Robj) -> extendr_api::Result<Robj> {
    let origin_chunks = as_point_chunks(origin)?;
    let dest_chunks = as_point_chunks(dest)?;
    let n = origin_chunks.iter().map(|c| c.len()).sum();
    check_pair_len(
        n,
        dest_chunks.iter().map(|c| c.len()).sum(),
        "origin",
        "dest",
    )?;
    let mut bldr = Float64Builder::with_capacity(n);

    for (orig, dst) in origin_chunks.iter().zip(dest_chunks.iter()) {
        dist_euclidean_impl(&mut bldr, orig, dst);
    }

    bldr.finish().into_arrow_robj()
}

fn dist_euclidean_impl(bldr: &mut Float64Builder, origin: &PointArray, dest: &PointArray) {
    for (xi, yi) in origin.iter().zip(dest.iter()) {
        if let (Some(Ok(x)), Some(Ok(y))) = (xi, yi) {
            let dist = Euclidean.distance(x.to_point(), y.to_point());
            bldr.append_value(dist);
        } else {
            bldr.append_null();
        }
    }
}

/// @export
/// @rdname dist_pairwise
/// @family distance
/// @references
///   [Distance](https://docs.rs/geo/latest/geo/algorithm/line_measures/trait.Distance.html),
///   [Haversine](https://docs.rs/geo/latest/geo/algorithm/line_measures/metric_spaces/constant.Haversine.html)
#[extendr]
fn ga_dist_haversine_pairwise(origin: Robj, dest: Robj) -> extendr_api::Result<Robj> {
    let origin_chunks = as_point_chunks(origin)?;
    let dest_chunks = as_point_chunks(dest)?;
    let n = origin_chunks.iter().map(|c| c.len()).sum();
    check_pair_len(
        n,
        dest_chunks.iter().map(|c| c.len()).sum(),
        "origin",
        "dest",
    )?;
    let mut bldr = Float64Builder::with_capacity(n);

    for (orig, dst) in origin_chunks.iter().zip(dest_chunks.iter()) {
        dist_haversine_impl(&mut bldr, orig, dst);
    }

    bldr.finish().into_arrow_robj()
}

fn dist_haversine_impl(bldr: &mut Float64Builder, origin: &PointArray, dest: &PointArray) {
    for (xi, yi) in origin.iter().zip(dest.iter()) {
        if let (Some(Ok(x)), Some(Ok(y))) = (xi, yi) {
            let dist = Haversine.distance(x.to_point(), y.to_point());
            bldr.append_value(dist);
        } else {
            bldr.append_null();
        }
    }
}

/// @export
/// @rdname dist_pairwise
/// @family distance
/// @references
///   [Distance](https://docs.rs/geo/latest/geo/algorithm/line_measures/trait.Distance.html),
///   [Geodesic](https://docs.rs/geo/latest/geo/algorithm/line_measures/metric_spaces/static.Geodesic.html)
#[extendr]
fn ga_dist_geodesic_pairwise(origin: Robj, dest: Robj) -> extendr_api::Result<Robj> {
    let origin_chunks = as_point_chunks(origin)?;
    let dest_chunks = as_point_chunks(dest)?;
    let n = origin_chunks.iter().map(|c| c.len()).sum();
    check_pair_len(
        n,
        dest_chunks.iter().map(|c| c.len()).sum(),
        "origin",
        "dest",
    )?;
    let mut bldr = Float64Builder::with_capacity(n);

    for (orig, dst) in origin_chunks.iter().zip(dest_chunks.iter()) {
        dist_geodesic_impl(&mut bldr, orig, dst);
    }

    bldr.finish().into_arrow_robj()
}

fn dist_geodesic_impl(bldr: &mut Float64Builder, origin: &PointArray, dest: &PointArray) {
    for (xi, yi) in origin.iter().zip(dest.iter()) {
        if let (Some(Ok(x)), Some(Ok(y))) = (xi, yi) {
            let dist = Geodesic.distance(x.to_point(), y.to_point());
            bldr.append_value(dist);
        } else {
            bldr.append_null();
        }
    }
}

/// @export
/// @rdname dist_pairwise
/// @family distance
/// @references
///   [Distance](https://docs.rs/geo/latest/geo/algorithm/line_measures/trait.Distance.html),
///   [Rhumb](https://docs.rs/geo/latest/geo/algorithm/line_measures/metric_spaces/struct.Rhumb.html)
#[extendr]
fn ga_dist_rhumb_pairwise(origin: Robj, dest: Robj) -> extendr_api::Result<Robj> {
    let origin_chunks = as_point_chunks(origin)?;
    let dest_chunks = as_point_chunks(dest)?;
    let n = origin_chunks.iter().map(|c| c.len()).sum();
    check_pair_len(
        n,
        dest_chunks.iter().map(|c| c.len()).sum(),
        "origin",
        "dest",
    )?;
    let mut bldr = Float64Builder::with_capacity(n);

    for (orig, dst) in origin_chunks.iter().zip(dest_chunks.iter()) {
        dist_rhumb_impl(&mut bldr, orig, dst);
    }

    bldr.finish().into_arrow_robj()
}

fn dist_rhumb_impl(bldr: &mut Float64Builder, origin: &PointArray, dest: &PointArray) {
    for (xi, yi) in origin.iter().zip(dest.iter()) {
        if let (Some(Ok(x)), Some(Ok(y))) = (xi, yi) {
            let dist = Rhumb.distance(x.to_point(), y.to_point());
            bldr.append_value(dist);
        } else {
            bldr.append_null();
        }
    }
}

/// Compute the pairwise Hausdorff distance between geometries
///
/// The Hausdorff distance measures how far two geometries are from each other
/// by taking the maximum of all minimum distances between points on the two shapes.
///
/// @param origin a GeoArrow geometry array
/// @param dest a GeoArrow geometry array
/// @returns a double vector of Hausdorff distance values
/// @export
/// @family distance
/// @references [HausdorffDistance](https://docs.rs/geo/latest/geo/algorithm/hausdorff_distance/trait.HausdorffDistance.html)
#[extendr]
fn ga_dist_hausdorff_pairwise(origin: Robj, dest: Robj) -> extendr_api::Result<Robj> {
    let origin_chunks = as_geometry_chunks(origin)?;
    let dest_chunks = as_geometry_chunks(dest)?;
    let n = origin_chunks.iter().map(|c| c.len()).sum();
    check_pair_len(
        n,
        dest_chunks.iter().map(|c| c.len()).sum(),
        "origin",
        "dest",
    )?;
    let mut bldr = Float64Builder::with_capacity(n);

    for (orig, dst) in origin_chunks.iter().zip(dest_chunks.iter()) {
        let origin = as_geo_geometries(orig.as_ref())?;
        let dest = as_geo_geometries(dst.as_ref())?;
        for (x, y) in origin.into_iter().zip(dest) {
            match (x, y) {
                (Some(x), Some(y)) => bldr.append_value(x.hausdorff_distance(&y)),
                _ => bldr.append_null(),
            }
        }
    }

    bldr.finish().into_arrow_robj()
}

/// Compute the pairwise Vincenty distance between points
///
/// The Vincenty formula computes the geodesic distance between two points on
/// an ellipsoidal model of the earth. Returns `NA` if the algorithm fails to
/// converge.
///
/// @param origin a GeoArrow point array of origin points
/// @param dest a GeoArrow point array of destination points
/// @returns a double vector of distance values in meters
/// @export
/// @family distance
/// @references [VincentyDistance](https://docs.rs/geo/latest/geo/algorithm/vincenty_distance/trait.VincentyDistance.html)
#[extendr]
fn ga_dist_vincenty_pairwise(origin: Robj, dest: Robj) -> extendr_api::Result<Robj> {
    let origin_chunks = as_point_chunks(origin)?;
    let dest_chunks = as_point_chunks(dest)?;
    let n = origin_chunks.iter().map(|c| c.len()).sum();
    check_pair_len(
        n,
        dest_chunks.iter().map(|c| c.len()).sum(),
        "origin",
        "dest",
    )?;
    let mut bldr = Float64Builder::with_capacity(n);

    for (orig, dst) in origin_chunks.iter().zip(dest_chunks.iter()) {
        dist_vincenty_impl(&mut bldr, orig, dst);
    }

    bldr.finish().into_arrow_robj()
}

fn dist_vincenty_impl(bldr: &mut Float64Builder, origin: &PointArray, dest: &PointArray) {
    for (xi, yi) in origin.iter().zip(dest.iter()) {
        if let (Some(Ok(x)), Some(Ok(y))) = (xi, yi) {
            match x.to_point().vincenty_distance(&y.to_point()) {
                Ok(dist) => bldr.append_value(dist),
                Err(_) => bldr.append_null(),
            }
        } else {
            bldr.append_null();
        }
    }
}

/// Compute the pairwise Frechet distance between linestrings
///
/// The Frechet distance measures the similarity between two curves by
/// considering the location and ordering of points along each curve.
/// Uses the Euclidean metric.
///
/// @param origin a GeoArrow linestring array
/// @param dest a GeoArrow linestring array
/// @returns a double vector of Frechet distance values
/// @export
/// @family distance
/// @references [FrechetDistance](https://docs.rs/geo/latest/geo/algorithm/line_measures/trait.FrechetDistance.html)
#[extendr]
fn ga_dist_frechet_pairwise(origin: Robj, dest: Robj) -> extendr_api::Result<Robj> {
    let origin_chunks = as_linestring_chunks(origin)?;
    let dest_chunks = as_linestring_chunks(dest)?;
    let n = origin_chunks.iter().map(|c| c.len()).sum();
    check_pair_len(
        n,
        dest_chunks.iter().map(|c| c.len()).sum(),
        "origin",
        "dest",
    )?;
    let mut bldr = Float64Builder::with_capacity(n);

    for (orig, dst) in origin_chunks.iter().zip(dest_chunks.iter()) {
        dist_frechet_impl(&mut bldr, orig, dst);
    }

    bldr.finish().into_arrow_robj()
}

fn dist_frechet_impl(bldr: &mut Float64Builder, origin: &LineStringArray, dest: &LineStringArray) {
    for (xi, yi) in origin.iter().zip(dest.iter()) {
        if let (Some(Ok(x)), Some(Ok(y))) = (xi, yi) {
            let dist = Euclidean.frechet_distance(&x.to_line_string(), &y.to_line_string());
            bldr.append_value(dist);
        } else {
            bldr.append_null();
        }
    }
}

extendr_module! {
    mod distance;
    fn ga_dist_euclidean_pairwise;
    fn ga_dist_haversine_pairwise;
    fn ga_dist_geodesic_pairwise;
    fn ga_dist_rhumb_pairwise;
    fn ga_dist_hausdorff_pairwise;
    fn ga_dist_vincenty_pairwise;
    fn ga_dist_frechet_pairwise;
}
