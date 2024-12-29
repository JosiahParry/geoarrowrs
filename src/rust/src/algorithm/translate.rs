use crate::{ffi::GeoChunks, HandleError};
use arrow::array::Float64Builder;
use extendr_api::prelude::*;
use geoarrow::{
    algorithm::{broadcasting::BroadcastablePrimitive, geo::Translate},
    array::NativeArrayDyn,
    chunked_array::ChunkedGeometryArray,
    NativeArray,
};

#[extendr]
pub fn translate_(x: GeoChunks, x_offset: Doubles, y_offset: Doubles) -> Result<GeoChunks> {
    let x_offset = match x_offset.len() > 1 {
        true => {
            let mut bldr = Float64Builder::with_capacity(x_offset.len());
            for xi in x_offset.into_iter() {
                match xi.is_na() {
                    true => bldr.append_null(),
                    false => bldr.append_value(xi.inner()),
                }
            }
            BroadcastablePrimitive::Array(bldr.finish())
        }
        false => BroadcastablePrimitive::Scalar(x_offset.elt(0).inner()),
    };

    let y_offset = match y_offset.len() > 1 {
        true => {
            let mut bldr = Float64Builder::with_capacity(y_offset.len());
            for yi in y_offset.into_iter() {
                match yi.is_na() {
                    true => bldr.append_null(),
                    false => bldr.append_value(yi.inner()),
                }
            }
            BroadcastablePrimitive::Array(bldr.finish())
        }
        false => BroadcastablePrimitive::Scalar(y_offset.elt(0).inner()),
    };

    let res =
        x.0.chunks()
            .into_iter()
            .map(|chunk| {
                Ok(NativeArrayDyn::new(
                    chunk
                        .as_ref()
                        .translate(&x_offset, &y_offset)
                        .handle_error()?,
                ))
            })
            .collect::<Result<Vec<_>>>()?;

    Ok(GeoChunks(ChunkedGeometryArray::new(res)))
}

extendr_module! {
    mod translate;
    fn translate_;
}
