use crate::{ffi::GeoChunks, HandleError};
use extendr_api::prelude::*;
use geoarrow::{
    algorithm::{broadcasting::BroadcastablePrimitive, geo::Scale},
    array::NativeArrayDyn,
    chunked_array::ChunkedGeometryArray,
    NativeArray,
};

#[extendr]
pub fn scale_(x: GeoChunks, scale_factor: f64) -> Result<GeoChunks> {
    let scale_factor = BroadcastablePrimitive::Scalar(scale_factor);

    let res =
        x.0.chunks()
            .into_iter()
            .map(|chunk| {
                Ok(NativeArrayDyn::new(
                    chunk.as_ref().scale(&scale_factor).handle_error()?,
                ))
            })
            .collect::<Result<Vec<_>>>()?;

    Ok(GeoChunks(ChunkedGeometryArray::new(res)))
}

#[extendr]
pub fn scale_xy_(x: GeoChunks, x_factor: f64, y_factor: f64) -> Result<GeoChunks> {
    let x_factor = BroadcastablePrimitive::Scalar(x_factor);
    let y_factor = BroadcastablePrimitive::Scalar(y_factor);

    let res =
        x.0.chunks()
            .into_iter()
            .map(|chunk| {
                Ok(NativeArrayDyn::new(
                    chunk
                        .as_ref()
                        .scale_xy(&x_factor, &y_factor)
                        .handle_error()?,
                ))
            })
            .collect::<Result<Vec<_>>>()?;

    Ok(GeoChunks(ChunkedGeometryArray::new(res)))
}

extendr_module! {
    mod scale;
    fn scale_;
    fn scale_xy_;
}
