use crate::{
    ffi::{GeoChunks, GeoChunksGeneric},
    HandleError,
};
use extendr_api::prelude::*;
use geoarrow::{
    algorithm::geo::InteriorPoint, array::PointArray, chunked_array::ChunkedGeometryArray,
    NativeArray,
};

#[extendr]
pub fn interior_point_(x: GeoChunks) -> Result<GeoChunksGeneric<PointArray>> {
    let chunks =
        x.0.chunks()
            .into_iter()
            .map(|chunk| {
                let res = chunk.as_ref().interior_point().handle_error()?;
                Ok(res)
            })
            .collect::<Result<Vec<_>>>()?;

    Ok(GeoChunksGeneric(ChunkedGeometryArray::new(chunks)))
}

extendr_module! {
    mod interior_point;
    fn interior_point_;
}
