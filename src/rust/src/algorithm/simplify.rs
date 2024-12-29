use crate::{ffi::GeoChunks, HandleError};
use extendr_api::prelude::*;
use geoarrow::{
    algorithm::{
        broadcasting::BroadcastablePrimitive,
        geo::{Simplify, SimplifyVw, SimplifyVwPreserve},
    },
    array::NativeArrayDyn,
    chunked_array::ChunkedGeometryArray,
    NativeArray,
};

#[extendr]
pub fn simplify_(x: GeoChunks, epsilon: f64) -> Result<GeoChunks> {
    let epsilon = BroadcastablePrimitive::Scalar(epsilon);
    let chunks =
        x.0.chunks()
            .into_iter()
            .map(|chunk| {
                Ok(NativeArrayDyn::new(
                    chunk.as_ref().simplify(&epsilon).handle_error()?,
                ))
            })
            .collect::<Result<Vec<_>>>()?;

    Ok(GeoChunks(ChunkedGeometryArray::new(chunks)))
}

#[extendr]
pub fn simplify_vw_(x: GeoChunks, epsilon: f64) -> Result<GeoChunks> {
    let epsilon = BroadcastablePrimitive::Scalar(epsilon);
    let chunks =
        x.0.chunks()
            .into_iter()
            .map(|chunk| {
                Ok(NativeArrayDyn::new(
                    chunk.as_ref().simplify_vw(&epsilon).handle_error()?,
                ))
            })
            .collect::<Result<Vec<_>>>()?;

    Ok(GeoChunks(ChunkedGeometryArray::new(chunks)))
}

#[extendr]
pub fn simplify_vw_preserve_(x: GeoChunks, epsilon: f64) -> Result<GeoChunks> {
    let epsilon = BroadcastablePrimitive::Scalar(epsilon);
    let chunks =
        x.0.chunks()
            .into_iter()
            .map(|chunk| {
                Ok(NativeArrayDyn::new(
                    chunk
                        .as_ref()
                        .simplify_vw_preserve(&epsilon)
                        .handle_error()?,
                ))
            })
            .collect::<Result<Vec<_>>>()?;

    Ok(GeoChunks(ChunkedGeometryArray::new(chunks)))
}

extendr_module! {
    mod simplify;
    fn simplify_;
    fn simplify_vw_;
    fn simplify_vw_preserve_;
}
