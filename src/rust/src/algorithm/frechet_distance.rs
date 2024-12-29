use arrow::datatypes::Float64Type;
use extendr_api::prelude::*;
use geoarrow::{algorithm::geo::FrechetDistance, chunked_array::ChunkedArray, NativeArray};

use crate::{
    ffi::{GeoChunks, PrimitiveChunks},
    HandleError,
};

#[extendr]
pub fn frechet_distance_pairwise_(
    x: GeoChunks,
    y: GeoChunks,
) -> Result<PrimitiveChunks<Float64Type>> {
    let chunks =
        x.0.chunks()
            .into_iter()
            .zip(y.0.chunks().into_iter())
            .map(|(lhs, rhs)| lhs.as_ref().frechet_distance(&rhs.as_ref()).handle_error())
            .collect::<Result<Vec<_>>>()?;

    Ok(PrimitiveChunks(ChunkedArray::new(chunks)))
}
extendr_module! {
    mod frechet_distance;
    fn frechet_distance_pairwise_;
}
