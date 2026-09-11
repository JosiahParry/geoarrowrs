use arrow::array::Float64Builder;
use arrow_extendr::IntoArrowRobj;
use extendr_api::prelude::*;
use geo::{
    Distance, Euclidean, Geodesic, HausdorffDistance, Haversine, LineString, Rhumb,
    VincentyDistance, line_measures::FrechetDistance,
};
use geo_traits::to_geo::ToGeoLineString;
use geoarrow::array::{GeoArrowArray, GeoArrowArrayAccessor};

use crate::{as_linestring_chunks, check_recycle_len, geometry_metric, point_metric};

/// Read a linestring argument as one linestring per row, recycled against `n`.
fn as_recycled_linestrings(
    robj: Robj,
    n: usize,
    label: &'static str,
) -> extendr_api::Result<Vec<Option<LineString<f64>>>> {
    let chunks = as_linestring_chunks(robj)?;
    let mut out = Vec::new();
    for chunk in &chunks {
        for item in chunk.iter() {
            out.push(match item {
                Some(Ok(l)) => Some(l.to_line_string()),
                _ => None,
            });
        }
    }
    check_recycle_len(out.len(), n, label)?;
    Ok(out)
}

/// Compute pairwise distances between points
///
/// Calculate the distance between each pair of geometries in `origin` and
/// `dest`. These functions differ in the metric space used for the calculation.
///
/// @details
/// `ga_dist_euclidean_pairwise()` measures between geometries of any type, so
/// the distance from a point to the nearest edge of a polygon is one call. The
/// spherical and ellipsoidal metrics take points only, because `geo` defines
/// them between points alone.
///
/// @param origin a GeoArrow geometry array for `ga_dist_euclidean_pairwise()`,
///   a point array for the others
/// @param dest a GeoArrow array matching `origin`; length 1 or the same length
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
    geometry_metric(origin, dest, |a, b| Euclidean.distance(a, b))
}

/// @export
/// @rdname dist_pairwise
/// @family distance
#[extendr]
fn ga_dist_haversine_pairwise(origin: Robj, dest: Robj) -> extendr_api::Result<Robj> {
    point_metric(origin, dest, |a, b| Some(Haversine.distance(*a, *b)))
}

/// @export
/// @rdname dist_pairwise
/// @family distance
#[extendr]
fn ga_dist_geodesic_pairwise(origin: Robj, dest: Robj) -> extendr_api::Result<Robj> {
    point_metric(origin, dest, |a, b| Some(Geodesic.distance(*a, *b)))
}

/// @export
/// @rdname dist_pairwise
/// @family distance
#[extendr]
fn ga_dist_rhumb_pairwise(origin: Robj, dest: Robj) -> extendr_api::Result<Robj> {
    point_metric(origin, dest, |a, b| Some(Rhumb.distance(*a, *b)))
}

/// Pairwise Hausdorff distance
///
/// The Hausdorff distance measures how far two geometries are from each other
/// by taking the maximum of all minimum distances between points on the two shapes.
///
/// @param origin a GeoArrow geometry array
/// @param dest a GeoArrow geometry array; length 1 or the same length as `origin`
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
    geometry_metric(origin, dest, |a, b| a.hausdorff_distance(b))
}

/// Pairwise Vincenty distance
///
/// The Vincenty formula computes the geodesic distance between two points on
/// an ellipsoidal model of the earth. Returns `NA` if the algorithm fails to
/// converge.
///
/// @param origin a GeoArrow point array of origin points
/// @param dest a GeoArrow point array; length 1 or the same length as `origin`
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
    point_metric(origin, dest, |a, b| a.vincenty_distance(b).ok())
}

/// Pairwise Frechet distance
///
/// The Frechet distance measures the similarity between two curves by
/// considering the location and ordering of points along each curve.
/// Uses the Euclidean metric.
///
/// @param origin a GeoArrow linestring array
/// @param dest a GeoArrow linestring array; length 1 or the same length as `origin`
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
    let n = origin_chunks.iter().map(|c| c.len()).sum();
    let dests = as_recycled_linestrings(dest, n, "dest")?;
    let mut bldr = Float64Builder::with_capacity(n);
    let mut dests = dests.iter().cycle();

    for chunk in &origin_chunks {
        for item in chunk.iter() {
            match (item, dests.next().and_then(|d| d.as_ref())) {
                (Some(Ok(x)), Some(y)) => {
                    bldr.append_value(Euclidean.frechet_distance(&x.to_line_string(), y))
                }
                _ => bldr.append_null(),
            }
        }
    }

    bldr.finish().into_arrow_robj()
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
