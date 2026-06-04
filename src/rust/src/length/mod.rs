use crate::as_linestring_chunks;
use arrow::array::Float64Builder;
use arrow_extendr::IntoArrowRobj;
use extendr_api::prelude::*;
use geo::{Euclidean, Geodesic, Haversine, Length, Rhumb};
use geo_traits::to_geo::ToGeoLineString;
use geoarrow::array::{GeoArrowArray, GeoArrowArrayAccessor, LineStringArray};
mod vicenty_length;

/// Compute the length of linestrings
///
/// These functions calculate the total length of each linestring using
/// different metric spaces. Use the geodesic or haversine variants for
/// geographic coordinates, and euclidean for projected coordinates.
///
/// @param x a GeoArrow linestring array
/// @returns a double vector of length values
/// @export
/// @rdname length
/// @family length
/// @references
///   [Length](https://docs.rs/geo/latest/geo/algorithm/line_measures/trait.Length.html),
///   [Euclidean](https://docs.rs/geo/latest/geo/algorithm/line_measures/metric_spaces/struct.Euclidean.html)
#[extendr]
fn length_euclidean(x: Robj) -> extendr_api::Result<Robj> {
    let x_chunks = as_linestring_chunks(x)?;
    let n = x_chunks.iter().map(|c| c.len()).sum();
    let mut bldr = Float64Builder::with_capacity(n);

    for x in x_chunks {
        length_euclidean_impl(&mut bldr, &x);
    }

    bldr.finish().into_arrow_robj()
}

fn length_euclidean_impl(bldr: &mut Float64Builder, x: &LineStringArray) {
    for xi in x.iter() {
        if let Some(Ok(val)) = xi {
            bldr.append_value(Euclidean.length(&val.to_line_string()))
        } else {
            bldr.append_null();
        }
    }
}

/// @export
/// @rdname length
/// @family length
/// @references
///   [Length](https://docs.rs/geo/latest/geo/algorithm/line_measures/trait.Length.html),
///   [Haversine](https://docs.rs/geo/latest/geo/algorithm/line_measures/metric_spaces/constant.Haversine.html)
#[extendr]
fn length_haversine(x: Robj) -> extendr_api::Result<Robj> {
    let x_chunks = as_linestring_chunks(x)?;
    let n = x_chunks.iter().map(|c| c.len()).sum();
    let mut bldr = Float64Builder::with_capacity(n);

    for x in x_chunks {
        length_haversine_impl(&mut bldr, &x);
    }

    bldr.finish().into_arrow_robj()
}

fn length_haversine_impl(bldr: &mut Float64Builder, x: &LineStringArray) {
    for xi in x.iter() {
        if let Some(Ok(val)) = xi {
            bldr.append_value(Haversine.length(&val.to_line_string()))
        } else {
            bldr.append_null();
        }
    }
}

/// @export
/// @rdname length
/// @family length
/// @references
///   [Length](https://docs.rs/geo/latest/geo/algorithm/line_measures/trait.Length.html),
///   [Geodesic](https://docs.rs/geo/latest/geo/algorithm/line_measures/metric_spaces/static.Geodesic.html)
#[extendr]
fn length_geodesic(x: Robj) -> extendr_api::Result<Robj> {
    let x_chunks = as_linestring_chunks(x)?;
    let n = x_chunks.iter().map(|c| c.len()).sum();
    let mut bldr = Float64Builder::with_capacity(n);

    for x in x_chunks {
        length_geodesic_impl(&mut bldr, &x);
    }

    bldr.finish().into_arrow_robj()
}

fn length_geodesic_impl(bldr: &mut Float64Builder, x: &LineStringArray) {
    for xi in x.iter() {
        if let Some(Ok(val)) = xi {
            bldr.append_value(Geodesic.length(&val.to_line_string()))
        } else {
            bldr.append_null();
        }
    }
}

/// @export
/// @rdname length
/// @family length
/// @references
///   [Length](https://docs.rs/geo/latest/geo/algorithm/line_measures/trait.Length.html),
///   [Rhumb](https://docs.rs/geo/latest/geo/algorithm/line_measures/metric_spaces/struct.Rhumb.html)
#[extendr]
fn length_rhumb(x: Robj) -> extendr_api::Result<Robj> {
    let x_chunks = as_linestring_chunks(x)?;
    let n = x_chunks.iter().map(|c| c.len()).sum();
    let mut bldr = Float64Builder::with_capacity(n);

    for x in x_chunks {
        length_rhumb_impl(&mut bldr, &x);
    }

    bldr.finish().into_arrow_robj()
}

fn length_rhumb_impl(bldr: &mut Float64Builder, x: &LineStringArray) {
    for xi in x.iter() {
        if let Some(Ok(val)) = xi {
            bldr.append_value(Rhumb.length(&val.to_line_string()))
        } else {
            bldr.append_null();
        }
    }
}

extendr_module! {
    mod length;
    fn length_euclidean;
    fn length_haversine;
    fn length_geodesic;
    fn length_rhumb;
    use vicenty_length;
}
