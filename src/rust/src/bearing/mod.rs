use extendr_api::prelude::*;
use geo::{Bearing, Euclidean, Geodesic, Haversine, Rhumb};

use crate::point_metric;

/// Compute the bearing between pairs of points
///
/// Calculate the bearing in degrees from `origin` to `dest` for each pair of
/// points. Bearing is measured clockwise from north (0 degrees) to 360 degrees.
/// These functions differ in the metric space used for the calculation.
///
/// @param origin a GeoArrow point array of origin points
/// @param dest a GeoArrow point array; length 1 or the same length as `origin`
/// @returns a double vector of bearing values in degrees
/// @export
/// @rdname bearing
/// @family bearing
/// @references
///   [Bearing](https://docs.rs/geo/latest/geo/algorithm/line_measures/trait.Bearing.html),
///   [Euclidean](https://docs.rs/geo/latest/geo/algorithm/line_measures/metric_spaces/struct.Euclidean.html),
///   [Haversine](https://docs.rs/geo/latest/geo/algorithm/line_measures/metric_spaces/constant.Haversine.html),
///   [Geodesic](https://docs.rs/geo/latest/geo/algorithm/line_measures/metric_spaces/static.Geodesic.html),
///   [Rhumb](https://docs.rs/geo/latest/geo/algorithm/line_measures/metric_spaces/struct.Rhumb.html)
/// @examplesIf requireNamespace("sf", quietly = TRUE) && requireNamespace("geoarrow", quietly = TRUE)
/// # Raleigh to Charlotte, and Raleigh to Wilmington
/// origin <- ga_xy(c(-78.6382, -78.6382), c(35.7796, 35.7796))
/// dest <- ga_xy(c(-80.8431, -77.9447), c(35.2271, 34.2257))
///
/// # west-southwest, then south-southeast
/// as.vector(ga_bearing_haversine(origin, dest))
/// as.vector(ga_bearing_rhumb(origin, dest))
// TODO: use rayon with min chunk size of 4096
#[extendr]
fn ga_bearing_euclidean(origin: Robj, dest: Robj) -> extendr_api::Result<Robj> {
    point_metric(origin, dest, |a, b| Some(Euclidean.bearing(*a, *b)))
}

/// @export
/// @rdname bearing
/// @family bearing
#[extendr]
fn ga_bearing_haversine(origin: Robj, dest: Robj) -> extendr_api::Result<Robj> {
    point_metric(origin, dest, |a, b| Some(Haversine.bearing(*a, *b)))
}

/// @export
/// @rdname bearing
/// @family bearing
#[extendr]
fn ga_bearing_geodesic(origin: Robj, dest: Robj) -> extendr_api::Result<Robj> {
    point_metric(origin, dest, |a, b| Some(Geodesic.bearing(*a, *b)))
}

/// @export
/// @rdname bearing
/// @family bearing
#[extendr]
fn ga_bearing_rhumb(origin: Robj, dest: Robj) -> extendr_api::Result<Robj> {
    point_metric(origin, dest, |a, b| Some(Rhumb.bearing(*a, *b)))
}

extendr_module! {
    mod bearing;
    fn ga_bearing_euclidean;
    fn ga_bearing_haversine;
    fn ga_bearing_geodesic;
    fn ga_bearing_rhumb;
}
