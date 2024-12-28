use crate::{ffi::GeoChunks, HandleError};
use extendr_api::prelude::*;
use geoarrow::{
    algorithm::geo::Densify, array::NativeArrayDyn, chunked_array::ChunkedGeometryArray,
    NativeArray,
};

#[extendr]
pub fn densify_(x: GeoChunks, max_distance: f64) -> Result<GeoChunks> {
    let res =
        x.0.chunks()
            .into_iter()
            .map(|chunk| {
                let res = chunk.as_ref().densify(max_distance).handle_error()?;
                Ok(NativeArrayDyn::new(res))
            })
            .collect::<Result<Vec<_>>>()?;

    let chunks = ChunkedGeometryArray::new(res);
    Ok(GeoChunks(chunks))
}

extendr_module! {
    mod densify;
    fn densify_;
}
