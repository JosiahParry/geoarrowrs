use std::collections::HashSet;

use crate::{
    ffi::{GeoChunks, PrimitiveChunks},
    HandleError,
};
use arrow::{array::Int16Array, datatypes::Int16Type};
use extendr_api::prelude::*;
use geoarrow::{
    algorithm::native::TypeIds, array::AsNativeArray, chunked_array::ChunkedArray,
    datatypes::NativeType, NativeArray,
};

#[extendr]
pub fn type_ids_(x: GeoChunks) -> Result<PrimitiveChunks<Int16Type>> {
    let inner = x.0;
    let dt = inner.data_type();

    let res = match dt {
        NativeType::Point(..) => inner
            .chunks()
            .into_iter()
            .map(|chunk| {
                Ok(chunk
                    .as_ref()
                    .as_point_opt()
                    .ok_or(Error::Other("Failed to cast to Point".to_string()))?
                    .get_type_ids())
            })
            .collect::<Result<Vec<_>>>()
            .handle_error()?,
        NativeType::LineString(..) => inner
            .chunks()
            .into_iter()
            .map(|chunk| {
                Ok(chunk
                    .as_ref()
                    .as_line_string_opt()
                    .ok_or(Error::Other("Failed to cast to LineString".to_string()))?
                    .get_type_ids())
            })
            .collect::<Result<Vec<_>>>()
            .handle_error()?,
        NativeType::Polygon(..) => inner
            .chunks()
            .into_iter()
            .map(|chunk| {
                Ok(chunk
                    .as_ref()
                    .as_polygon_opt()
                    .ok_or(Error::Other("Failed to cast to Polygon".to_string()))?
                    .get_type_ids())
            })
            .collect::<Result<Vec<_>>>()
            .handle_error()?,
        NativeType::MultiPoint(..) => inner
            .chunks()
            .into_iter()
            .map(|chunk| {
                Ok(chunk
                    .as_ref()
                    .as_multi_point_opt()
                    .ok_or(Error::Other("Failed to cast to MultiPoint".to_string()))?
                    .get_type_ids())
            })
            .collect::<Result<Vec<_>>>()
            .handle_error()?,
        NativeType::MultiLineString(..) => inner
            .chunks()
            .into_iter()
            .map(|chunk| {
                Ok(chunk
                    .as_ref()
                    .as_multi_line_string_opt()
                    .ok_or(Error::Other(
                        "Failed to cast to MultiLineString".to_string(),
                    ))?
                    .get_type_ids())
            })
            .collect::<Result<Vec<_>>>()
            .handle_error()?,
        NativeType::MultiPolygon(..) => inner
            .chunks()
            .into_iter()
            .map(|chunk| {
                Ok(chunk
                    .as_ref()
                    .as_multi_polygon_opt()
                    .ok_or(Error::Other("Failed to cast to MultiPolygon".to_string()))?
                    .get_type_ids())
            })
            .collect::<Result<Vec<_>>>()
            .handle_error()?,
        _ => {
            return Err(Error::Other(String::from(
                "Not implemented for this geometry type",
            )));
        }
    };
    Ok(PrimitiveChunks(ChunkedArray::new(res)))
}

#[extendr]
pub fn type_ids_unique_(x: GeoChunks) -> Result<PrimitiveChunks<Int16Type>> {
    let inner = x.0;
    let dt = inner.data_type();

    let res = match dt {
        NativeType::Point(..) => inner
            .chunks()
            .into_iter()
            .map(|chunk| {
                Ok(chunk
                    .as_ref()
                    .as_point_opt()
                    .ok_or(Error::Other("Failed to cast to Point".to_string()))?
                    .get_unique_type_ids())
            })
            .collect::<Result<Vec<_>>>()
            .handle_error()?,
        NativeType::LineString(..) => inner
            .chunks()
            .into_iter()
            .map(|chunk| {
                Ok(chunk
                    .as_ref()
                    .as_line_string_opt()
                    .ok_or(Error::Other("Failed to cast to LineString".to_string()))?
                    .get_unique_type_ids())
            })
            .collect::<Result<Vec<_>>>()
            .handle_error()?,
        NativeType::Polygon(..) => inner
            .chunks()
            .into_iter()
            .map(|chunk| {
                Ok(chunk
                    .as_ref()
                    .as_polygon_opt()
                    .ok_or(Error::Other("Failed to cast to Polygon".to_string()))?
                    .get_unique_type_ids())
            })
            .collect::<Result<Vec<_>>>()
            .handle_error()?,
        NativeType::MultiPoint(..) => inner
            .chunks()
            .into_iter()
            .map(|chunk| {
                Ok(chunk
                    .as_ref()
                    .as_multi_point_opt()
                    .ok_or(Error::Other("Failed to cast to MultiPoint".to_string()))?
                    .get_unique_type_ids())
            })
            .collect::<Result<Vec<_>>>()
            .handle_error()?,
        NativeType::MultiLineString(..) => inner
            .chunks()
            .into_iter()
            .map(|chunk| {
                Ok(chunk
                    .as_ref()
                    .as_multi_line_string_opt()
                    .ok_or(Error::Other(
                        "Failed to cast to MultiLineString".to_string(),
                    ))?
                    .get_unique_type_ids())
            })
            .collect::<Result<Vec<_>>>()
            .handle_error()?,
        NativeType::MultiPolygon(..) => inner
            .chunks()
            .into_iter()
            .map(|chunk| {
                Ok(chunk
                    .as_ref()
                    .as_multi_polygon_opt()
                    .ok_or(Error::Other("Failed to cast to MultiPolygon".to_string()))?
                    .get_unique_type_ids())
            })
            .collect::<Result<Vec<_>>>()
            .handle_error()?,
        _ => {
            return Err(Error::Other(String::from(
                "Not implemented for this geometry type",
            )));
        }
    };

    // merge the hashsets from all of the chunks
    let mut final_set = HashSet::new();

    for set in res {
        final_set.extend(set);
    }

    let mut final_vec = final_set.into_iter().collect::<Vec<_>>();
    final_vec.sort();

    // return the final hashset as a chunked array with onyl one chunk
    let res = Int16Array::from_iter_values(final_vec);

    Ok(PrimitiveChunks(ChunkedArray::new(vec![res])))
}

extendr_module! {
    mod type_ids;
    fn type_ids_;
    fn type_ids_unique_;
}
