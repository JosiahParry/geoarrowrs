use arrow::datatypes::Float64Type;
use extendr_api::prelude::*;
use geoarrow::algorithm::geo::{Area, GeodesicArea};

use crate::{
    ffi::{GeoChunks, PrimitiveChunks},
    HandleError,
};

#[extendr]
pub fn area_euclidean_unsigned_(x: GeoChunks) -> Result<PrimitiveChunks<Float64Type>> {
    let y = x.clone();
    let unsigned_area = y.0.unsigned_area().handle_error()?;
    let res = PrimitiveChunks(unsigned_area);
    Ok(res)
}

#[extendr]
pub fn area_geodesic_unsigned_(x: GeoChunks) -> Result<PrimitiveChunks<Float64Type>> {
    Ok(PrimitiveChunks(
        x.0.geodesic_area_unsigned().handle_error()?,
    ))
}

extendr_module! {
    mod area;
    fn area_euclidean_unsigned_;
    fn area_geodesic_unsigned_;
}
