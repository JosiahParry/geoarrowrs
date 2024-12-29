use crate::{ffi::GeoChunks, HandleError};
use extendr_api::prelude::*;
use geoarrow::{
    algorithm::geo::Rotate, array::NativeArrayDyn, chunked_array::ChunkedGeometryArray, NativeArray,
};

#[extendr]
pub fn rotate_around_center_(x: GeoChunks, degrees: f64) -> Result<GeoChunks> {
    let res =
        x.0.chunks()
            .into_iter()
            .map(|chunk| {
                Ok(NativeArrayDyn::new(
                    chunk
                        .as_ref()
                        .rotate_around_center(&degrees)
                        .handle_error()?,
                ))
            })
            .collect::<Result<Vec<_>>>()?;
    Ok(GeoChunks(ChunkedGeometryArray::new(res)))
}

#[extendr]
pub fn rotate_around_centroid_(x: GeoChunks, degrees: f64) -> Result<GeoChunks> {
    let res =
        x.0.chunks()
            .into_iter()
            .map(|chunk| {
                Ok(NativeArrayDyn::new(
                    chunk
                        .as_ref()
                        .rotate_around_centroid(&degrees)
                        .handle_error()?,
                ))
            })
            .collect::<Result<Vec<_>>>()?;
    Ok(GeoChunks(ChunkedGeometryArray::new(res)))
}

extendr_module! {
    mod rotate;
    fn rotate_around_centroid_;
    fn rotate_around_center_;
}
