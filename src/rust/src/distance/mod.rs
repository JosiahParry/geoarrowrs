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
///   [Euclidean](https://docs.rs/geo/latest/geo/algorithm/line_measures/metric_spaces/struct.Euclidean.html),
///   [Haversine](https://docs.rs/geo/latest/geo/algorithm/line_measures/metric_spaces/constant.Haversine.html),
///   [Geodesic](https://docs.rs/geo/latest/geo/algorithm/line_measures/metric_spaces/static.Geodesic.html),
///   [Rhumb](https://docs.rs/geo/latest/geo/algorithm/line_measures/metric_spaces/struct.Rhumb.html)
/// @examplesIf requireNamespace("sf", quietly = TRUE) && requireNamespace("geoarrow", quietly = TRUE)
/// # Raleigh to Charlotte, and Raleigh to Wilmington
/// origin <- ga_xy(c(-78.6382, -78.6382), c(35.7796, 35.7796))
/// dest <- ga_xy(c(-80.8431, -77.9447), c(35.2271, 34.2257))
///
/// # degrees, then meters
/// as.vector(ga_dist_euclidean_pairwise(origin, dest))
/// as.vector(ga_dist_geodesic_pairwise(origin, dest))
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

/// Pairwise Hausdorff distance
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
/// @examplesIf requireNamespace("sf", quietly = TRUE) && requireNamespace("geoarrow", quietly = TRUE)
/// # how far apart are neighbouring counties at their worst?
/// fp <- system.file("shape/nc.shp", package = "sf")
/// nc <- as.data.frame(read_shapefile(fp))
///
/// as.vector(ga_dist_hausdorff_pairwise(nc$geometry[1:5], nc$geometry[2:6]))
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

/// Pairwise Vincenty distance
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
/// @examplesIf requireNamespace("sf", quietly = TRUE) && requireNamespace("geoarrow", quietly = TRUE)
/// # Raleigh to Charlotte, and Raleigh to Wilmington
/// origin <- ga_xy(c(-78.6382, -78.6382), c(35.7796, 35.7796))
/// dest <- ga_xy(c(-80.8431, -77.9447), c(35.2271, 34.2257))
///
/// as.vector(ga_dist_vincenty_pairwise(origin, dest))
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

/// Pairwise Frechet distance
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
/// @examplesIf requireNamespace("sf", quietly = TRUE) && requireNamespace("geoarrow", quietly = TRUE)
/// # a straight route against one that detours north
/// direct <- geoarrow::as_geoarrow_array(sf::st_sfc(
///   sf::st_linestring(rbind(c(0, 0), c(2, 0), c(4, 0)))
/// ))
/// detour <- geoarrow::as_geoarrow_array(sf::st_sfc(
///   sf::st_linestring(rbind(c(0, 0), c(2, 1), c(4, 0)))
/// ))
///
/// as.vector(ga_dist_frechet_pairwise(direct, detour))
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
