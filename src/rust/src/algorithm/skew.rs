use crate::{ffi::GeoChunks, HandleError};
use extendr_api::prelude::*;
use geoarrow::{
    algorithm::{broadcasting::BroadcastablePrimitive, geo::Skew},
    array::NativeArrayDyn,
    chunked_array::ChunkedGeometryArray,
    NativeArray,
};

#[extendr]
pub fn skew_(x: GeoChunks, degrees: f64) -> Result<GeoChunks> {
    let degrees = BroadcastablePrimitive::Scalar(degrees);

    let res =
        x.0.chunks()
            .into_iter()
            .map(|chunk| {
                Ok(NativeArrayDyn::new(
                    chunk.as_ref().skew(&degrees).handle_error()?,
                ))
            })
            .collect::<Result<Vec<_>>>()?;

    Ok(GeoChunks(ChunkedGeometryArray::new(res)))
}

#[extendr]
pub fn skew_xy_(x: GeoChunks, degrees_x: f64, degrees_y: f64) -> Result<GeoChunks> {
    let degrees_x = BroadcastablePrimitive::Scalar(degrees_x);
    let degrees_y = BroadcastablePrimitive::Scalar(degrees_y);

    let res =
        x.0.chunks()
            .into_iter()
            .map(|chunk| {
                Ok(NativeArrayDyn::new(
                    chunk
                        .as_ref()
                        .skew_xy(&degrees_x, &degrees_y)
                        .handle_error()?,
                ))
            })
            .collect::<Result<Vec<_>>>()?;

    Ok(GeoChunks(ChunkedGeometryArray::new(res)))
}

extendr_module! {
    mod skew;
    fn skew_;
    fn skew_xy_;
}
