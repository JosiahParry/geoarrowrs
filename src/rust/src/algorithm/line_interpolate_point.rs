use crate::{
    ffi::{GeoChunks, GeoChunksGeneric},
    HandleError,
};
use extendr_api::prelude::*;
use geoarrow::{
    algorithm::geo::LineInterpolatePoint, array::PointArray, chunked_array::ChunkedGeometryArray,
    NativeArray,
};

#[extendr]
pub fn line_interpolate_point_(
    x: GeoChunks,
    fraction: f64,
) -> Result<GeoChunksGeneric<PointArray>> {
    let chunks =
        x.0.chunks()
            .into_iter()
            .map(|chunk| {
                let res = chunk
                    .as_ref()
                    .line_interpolate_point(fraction)
                    .handle_error()?;
                Ok(res)
            })
            .collect::<Result<Vec<_>>>()?;

    Ok(GeoChunksGeneric(ChunkedGeometryArray::new(chunks)))
}

extendr_module! {
    mod line_interpolate_point;
    fn line_interpolate_point_;
}
