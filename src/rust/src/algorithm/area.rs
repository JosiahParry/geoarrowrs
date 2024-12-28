use crate::{
    ffi::{GeoChunks, PrimitiveChunks},
    HandleError,
};
use arrow::datatypes::Float64Type;
use extendr_api::prelude::*;
use geoarrow::algorithm::geo::{Area, ChamberlainDuquetteArea, GeodesicArea};

// Euclidean
#[extendr]
pub fn area_euclidean_unsigned_(x: GeoChunks) -> Result<PrimitiveChunks<Float64Type>> {
    let unsigned_area = x.0.unsigned_area().handle_error()?;
    let res = PrimitiveChunks(unsigned_area);
    Ok(res)
}

#[extendr]
pub fn area_euclidean_signed_(x: GeoChunks) -> Result<PrimitiveChunks<Float64Type>> {
    let unsigned_area = x.0.signed_area().handle_error()?;
    let res = PrimitiveChunks(unsigned_area);
    Ok(res)
}

// Geodesic
#[extendr]
pub fn area_geodesic_unsigned_(x: GeoChunks) -> Result<PrimitiveChunks<Float64Type>> {
    Ok(PrimitiveChunks(
        x.0.geodesic_area_unsigned().handle_error()?,
    ))
}

#[extendr]
pub fn area_geodesic_signed_(x: GeoChunks) -> Result<PrimitiveChunks<Float64Type>> {
    Ok(PrimitiveChunks(x.0.geodesic_area_signed().handle_error()?))
}

// ChamberlainDuquette
#[extendr]
pub fn area_chamberlain_duquette_signed_(x: GeoChunks) -> Result<PrimitiveChunks<Float64Type>> {
    Ok(PrimitiveChunks(
        x.0.chamberlain_duquette_signed_area().handle_error()?,
    ))
}

#[extendr]
pub fn area_chamberlain_duquette_unsigned_(x: GeoChunks) -> Result<PrimitiveChunks<Float64Type>> {
    Ok(PrimitiveChunks(
        x.0.chamberlain_duquette_signed_area().handle_error()?,
    ))
}

extendr_module! {
    mod area;
    fn area_euclidean_unsigned_;
    fn area_euclidean_signed_;
    fn area_geodesic_unsigned_;
    fn area_geodesic_signed_;
    fn area_chamberlain_duquette_signed_;
    fn area_chamberlain_duquette_unsigned_;
}
