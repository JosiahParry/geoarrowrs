use crate::{
    ffi::{GeoChunks, PrimitiveChunks},
    HandleError,
};
use arrow::datatypes::Float64Type;
use extendr_api::prelude::*;
use geoarrow::{algorithm::geo::LineLocatePoint, chunked_array::ChunkedArray, NativeArray};

#[extendr]
pub fn line_locate_point_(x: GeoChunks, y: GeoChunks) -> Result<PrimitiveChunks<Float64Type>> {
    let chunks =
        x.0.chunks()
            .into_iter()
            .zip(y.0.chunks().into_iter())
            .map(|(lhs, rhs)| {
                let res = lhs.as_ref().line_locate_point(rhs).handle_error()?;
                Ok(res)
            })
            .collect::<Result<Vec<_>>>()?;

    Ok(PrimitiveChunks(ChunkedArray::new(chunks)))
}

extendr_module! {
    mod line_locate_point;
    fn line_locate_point_;
}
