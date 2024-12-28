use crate::ffi::GeoChunksGeneric;
use crate::{ffi::GeoChunks, HandleError};
use extendr_api::prelude::*;
use geoarrow::{
    algorithm::{broadcasting::BroadcastablePrimitive, geo::ConcaveHull},
    array::PolygonArray,
    chunked_array::ChunkedPolygonArray,
};

#[extendr]
pub fn concave_hull_(x: GeoChunks, concavity: f64) -> Result<GeoChunksGeneric<PolygonArray>> {
    let concavity = BroadcastablePrimitive::Scalar(concavity);
    let inner = x.0;
    let res = inner
        .chunks()
        .into_iter()
        .map(|chunk| {
            let inn = chunk.inner();

            let res = inn.as_ref().concave_hull(&concavity).handle_error()?;
            Ok(res)
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(GeoChunksGeneric(ChunkedPolygonArray::new(res)))
}

extendr_module! {
    mod concave_hull;
    fn concave_hull_;
}
