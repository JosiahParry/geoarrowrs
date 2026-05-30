use std::sync::Arc;

use arrow::array::{ArrayData, Float64Array};
use arrow_extendr::{FromArrowRobj, geoarrow::GeoArrowVctr};
use extendr_api::prelude::*;
use geoarrow::array::{
    LineStringArray, MultiLineStringArray, MultiPolygonArray, PointArray, PolygonArray,
};
use geoarrow_array::GeoArrowArray;

pub(crate) mod area;
pub(crate) mod bearing;
pub(crate) mod densify;
pub(crate) mod destination;
pub(crate) mod distance;
pub(crate) mod interpolate_line;
pub(crate) mod interpolate_point;
pub(crate) mod length;

fn as_point_chunks(x: Robj) -> extendr_api::Result<Vec<PointArray>> {
    if let Ok(vctr) = GeoArrowVctr::try_from(&x) {
        vctr.as_point_chunks()
            .map_err(|e| Error::Other(e.to_string()))
    } else {
        let arr = PointArray::from_arrow_robj(&x).map_err(|e| Error::Other(e.to_string()))?;
        Ok(vec![arr])
    }
}

fn as_geometry_chunks(x: Robj) -> extendr_api::Result<Vec<Arc<dyn GeoArrowArray>>> {
    if let Ok(vctr) = GeoArrowVctr::try_from(&x) {
        vctr.as_dyn_chunks()
            .map_err(|e| Error::Other(e.to_string()))
    } else {
        let arr = <Arc<dyn GeoArrowArray>>::from_arrow_robj(&x)
            .map_err(|e| Error::Other(e.to_string()))?;
        Ok(vec![arr])
    }
}

fn as_polygon_chunks(x: Robj) -> extendr_api::Result<Vec<PolygonArray>> {
    if let Ok(vctr) = GeoArrowVctr::try_from(&x) {
        vctr.as_polygon_chunks()
            .map_err(|e| Error::Other(e.to_string()))
    } else {
        let arr = PolygonArray::from_arrow_robj(&x).map_err(|e| Error::Other(e.to_string()))?;
        Ok(vec![arr])
    }
}

fn as_multipolygon_chunks(x: Robj) -> extendr_api::Result<Vec<MultiPolygonArray>> {
    if let Ok(vctr) = GeoArrowVctr::try_from(&x) {
        vctr.as_multipolygon_chunks()
            .map_err(|e| Error::Other(e.to_string()))
    } else {
        let arr =
            MultiPolygonArray::from_arrow_robj(&x).map_err(|e| Error::Other(e.to_string()))?;
        Ok(vec![arr])
    }
}

fn as_linestring_chunks(x: Robj) -> extendr_api::Result<Vec<LineStringArray>> {
    if let Ok(vctr) = GeoArrowVctr::try_from(&x) {
        vctr.as_linestring_chunks()
            .map_err(|e| Error::Other(e.to_string()))
    } else {
        let arr = LineStringArray::from_arrow_robj(&x).map_err(|e| Error::Other(e.to_string()))?;
        Ok(vec![arr])
    }
}
fn as_multilinestring_chunks(x: Robj) -> extendr_api::Result<Vec<MultiLineStringArray>> {
    if let Ok(vctr) = GeoArrowVctr::try_from(&x) {
        vctr.as_multilinestring_chunks()
            .map_err(|e| Error::Other(e.to_string()))
    } else {
        let arr =
            MultiLineStringArray::from_arrow_robj(&x).map_err(|e| Error::Other(e.to_string()))?;
        Ok(vec![arr])
    }
}

pub(crate) fn try_float_array(
    robj: Robj,
    label: &'static str,
) -> extendr_api::Result<Float64Array> {
    let err_string = format!("Expected `{label}` to be float 64 array");
    let Ok(arr) = ArrayData::from_arrow_robj(&robj) else {
        return Err(Error::Other(err_string));
    };

    if !arr.data_type().is_floating() {
        return Err(Error::Other(err_string));
    }

    Ok(Float64Array::from(arr))
}
// Macro to generate exports.
// This ensures exported functions are registered with R.
// See corresponding C code in `entrypoint.c`.
extendr_module! {
    mod geoarrowrs;
    use area;
    use distance;
    use length;
    use bearing;
    use destination;
}
