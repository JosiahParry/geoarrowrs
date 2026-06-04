use arrow::array::Float64Builder;
use arrow_extendr::IntoArrowRobj;
use extendr_api::prelude::*;
use geo::VincentyLength;
use geo_traits::to_geo::{ToGeoLineString, ToGeoMultiLineString};
use geoarrow::array::{
    GeoArrowArray, GeoArrowArrayAccessor, LineStringArray, MultiLineStringArray,
};

use crate::{as_linestring_chunks, as_multilinestring_chunks};

/// Compute the Vincenty length of linestrings or multilinestrings
///
/// Calculates the geodesic length of each linestring or multilinestring using
/// the Vincenty formula on an ellipsoidal model of the earth. Returns `NA`
/// if the algorithm fails to converge. Results are in meters.
///
/// @param x a GeoArrow linestring or multilinestring array
/// @returns a double vector of length values in meters
/// @export
/// @rdname length
/// @family length
/// @references [VincentyLength](https://docs.rs/geo/latest/geo/algorithm/vincenty_length/trait.VincentyLength.html)
#[extendr]
fn length_vincenty(x: Robj) -> extendr_api::Result<Robj> {
    if let Ok(chunks) = as_linestring_chunks(x.clone()) {
        let n = chunks.iter().map(|c| c.len()).sum();
        let mut bldr = Float64Builder::with_capacity(n);
        for chunk in &chunks {
            length_vincenty_linestring_impl(&mut bldr, chunk);
        }
        return bldr.finish().into_arrow_robj();
    }

    if let Ok(chunks) = as_multilinestring_chunks(x) {
        let n = chunks.iter().map(|c| c.len()).sum();
        let mut bldr = Float64Builder::with_capacity(n);
        for chunk in &chunks {
            length_vincenty_multilinestring_impl(&mut bldr, chunk);
        }
        return bldr.finish().into_arrow_robj();
    }

    Err(Error::Other(
        "Expected linestrings or multilinestrings".to_string(),
    ))
}

fn length_vincenty_linestring_impl(bldr: &mut Float64Builder, x: &LineStringArray) {
    for xi in x.iter() {
        if let Some(Ok(val)) = xi {
            match val.to_line_string().vincenty_length() {
                Ok(len) => bldr.append_value(len),
                Err(_) => bldr.append_null(),
            }
        } else {
            bldr.append_null();
        }
    }
}

fn length_vincenty_multilinestring_impl(bldr: &mut Float64Builder, x: &MultiLineStringArray) {
    for xi in x.iter() {
        if let Some(Ok(val)) = xi {
            match val.to_multi_line_string().vincenty_length() {
                Ok(len) => bldr.append_value(len),
                Err(_) => bldr.append_null(),
            }
        } else {
            bldr.append_null();
        }
    }
}

extendr_module! {
    mod vicenty_length;
    fn length_vincenty;
}
