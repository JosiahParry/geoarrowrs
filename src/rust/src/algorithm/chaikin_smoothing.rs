use crate::{ffi::GeoChunks, HandleError};
use extendr_api::prelude::*;
use geoarrow::chunked_array::ChunkedGeometryArray;
use geoarrow::{algorithm::geo::ChaikinSmoothing, array::NativeArrayDyn};

#[extendr]
pub fn chaikin_smoothing_(x: GeoChunks, n_iterations: usize) -> Result<GeoChunks> {
    let inner = x.0;
    let smoothed_chunk = inner
        .chunks()
        .into_iter()
        .map(|chunk| {
            let inn = chunk.inner();
            let res = inn
                .as_ref()
                .chaikin_smoothing(n_iterations as u32)
                .handle_error()?;
            Ok(NativeArrayDyn::new(res))
        })
        .collect::<Result<Vec<_>>>()?;
    let res = ChunkedGeometryArray::new(smoothed_chunk);
    Ok(GeoChunks(res))
}

extendr_module! {
    mod chaikin_smoothing;
    fn chaikin_smoothing_;
}
