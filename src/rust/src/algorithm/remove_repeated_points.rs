use crate::{ffi::GeoChunks, HandleError};
use extendr_api::prelude::*;
use geoarrow::{
    algorithm::geo::RemoveRepeatedPoints, array::NativeArrayDyn,
    chunked_array::ChunkedGeometryArray, NativeArray,
};

#[extendr]
pub fn remove_repeated_points_(x: GeoChunks) -> Result<GeoChunks> {
    let chunks =
        x.0.chunks()
            .into_iter()
            .map(|chunk| {
                Ok(NativeArrayDyn::new(
                    chunk.as_ref().remove_repeated_points().handle_error()?,
                ))
            })
            .collect::<Result<Vec<_>>>()?;

    Ok(GeoChunks(ChunkedGeometryArray::new(chunks)))
}

extendr_module! {
    mod remove_repeated_points;
    fn remove_repeated_points_;
}
