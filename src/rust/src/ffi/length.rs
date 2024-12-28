use crate::{
    ffi::{GeoChunks, PrimitiveChunks},
    HandleError,
};
use arrow::{array::Float64Array, datatypes::Float64Type};
use extendr_api::prelude::*;
use geoarrow::{
    algorithm::geo::{EuclideanLength, GeodesicLength, HaversineLength, VincentyLength},
    chunked_array::ChunkedArray,
    io::geozero::{ToLineStringArray, ToMultiLineStringArray, ToMultiPointArray, ToPointArray},
    ArrayBase,
};

#[extendr]
pub fn length_haversine_(x: GeoChunks) -> Result<PrimitiveChunks<Float64Type>> {
    let inner = x.0;
    let geo_type = inner.data_type();
    let res_array = match geo_type {
        geoarrow::datatypes::NativeType::Point(_, dimension) => {
            let res_chunks = inner
                .chunks()
                .into_iter()
                .map(|chunk| {
                    let lines = chunk.to_point_array(dimension).handle_error()?;
                    let res = lines.haversine_length();
                    Ok(res)
                })
                .collect::<Result<Vec<_>>>();
            ChunkedArray::new(res_chunks?)
        }
        geoarrow::datatypes::NativeType::LineString(_, dimension) => {
            let res_chunks: std::result::Result<
                Vec<arrow::array::PrimitiveArray<Float64Type>>,
                Error,
            > = inner
                .chunks()
                .into_iter()
                .map(|chunk| {
                    let lines = chunk.to_line_string_array(dimension).handle_error()?;
                    let res = lines.haversine_length();
                    Ok(res)
                })
                .collect::<Result<Vec<_>>>();
            ChunkedArray::new(res_chunks?)
        }
        geoarrow::datatypes::NativeType::MultiPoint(_, dimension) => {
            let res_chunks = inner
                .chunks()
                .into_iter()
                .map(|chunk| {
                    let lines = chunk.to_multi_point_array(dimension).handle_error()?;
                    let res = lines.haversine_length();
                    Ok(res)
                })
                .collect::<Result<Vec<_>>>();
            ChunkedArray::new(res_chunks?)
        }
        geoarrow::datatypes::NativeType::MultiLineString(_, dimension) => {
            let res_chunks: std::result::Result<
                Vec<arrow::array::PrimitiveArray<Float64Type>>,
                Error,
            > = inner
                .chunks()
                .into_iter()
                .map(|chunk| {
                    let lines = chunk.to_multi_line_string_array(dimension).handle_error()?;
                    let res = lines.haversine_length();
                    Ok(res)
                })
                .collect::<Result<Vec<_>>>();
            ChunkedArray::new(res_chunks?)
        }
        _ => {
            let res_chunks = inner
                .chunks()
                .into_iter()
                .map(|chunk| {
                    let res = Float64Array::new_null(chunk.len());
                    Ok(res)
                })
                .collect::<Result<Vec<_>>>();
            ChunkedArray::new(res_chunks?)
        }
    };

    Ok(PrimitiveChunks(res_array))
}

#[extendr]
pub fn length_geodesic_(x: GeoChunks) -> Result<PrimitiveChunks<Float64Type>> {
    let inner = x.0;
    let geo_type = inner.data_type();
    let res_array = match geo_type {
        geoarrow::datatypes::NativeType::Point(_, dimension) => {
            let res_chunks = inner
                .chunks()
                .into_iter()
                .map(|chunk| {
                    let lines = chunk.to_point_array(dimension).handle_error()?;
                    let res = lines.geodesic_length();
                    Ok(res)
                })
                .collect::<Result<Vec<_>>>();
            ChunkedArray::new(res_chunks?)
        }
        geoarrow::datatypes::NativeType::LineString(_, dimension) => {
            let res_chunks: std::result::Result<
                Vec<arrow::array::PrimitiveArray<Float64Type>>,
                Error,
            > = inner
                .chunks()
                .into_iter()
                .map(|chunk| {
                    let lines = chunk.to_line_string_array(dimension).handle_error()?;
                    let res = lines.geodesic_length();
                    Ok(res)
                })
                .collect::<Result<Vec<_>>>();
            ChunkedArray::new(res_chunks?)
        }
        geoarrow::datatypes::NativeType::MultiPoint(_, dimension) => {
            let res_chunks = inner
                .chunks()
                .into_iter()
                .map(|chunk| {
                    let lines = chunk.to_multi_point_array(dimension).handle_error()?;
                    let res = lines.geodesic_length();
                    Ok(res)
                })
                .collect::<Result<Vec<_>>>();
            ChunkedArray::new(res_chunks?)
        }
        geoarrow::datatypes::NativeType::MultiLineString(_, dimension) => {
            let res_chunks: std::result::Result<
                Vec<arrow::array::PrimitiveArray<Float64Type>>,
                Error,
            > = inner
                .chunks()
                .into_iter()
                .map(|chunk| {
                    let lines = chunk.to_multi_line_string_array(dimension).handle_error()?;
                    let res = lines.geodesic_length();
                    Ok(res)
                })
                .collect::<Result<Vec<_>>>();
            ChunkedArray::new(res_chunks?)
        }
        _ => {
            let res_chunks = inner
                .chunks()
                .into_iter()
                .map(|chunk| {
                    let res = Float64Array::new_null(chunk.len());
                    Ok(res)
                })
                .collect::<Result<Vec<_>>>();
            ChunkedArray::new(res_chunks?)
        }
    };

    Ok(PrimitiveChunks(res_array))
}

#[extendr]
pub fn length_euclidean_(x: GeoChunks) -> Result<PrimitiveChunks<Float64Type>> {
    let inner = x.0;
    let geo_type = inner.data_type();
    let res_array = match geo_type {
        geoarrow::datatypes::NativeType::Point(_, dimension) => {
            let res_chunks = inner
                .chunks()
                .into_iter()
                .map(|chunk| {
                    let lines = chunk.to_point_array(dimension).handle_error()?;
                    let res = lines.euclidean_length();
                    Ok(res)
                })
                .collect::<Result<Vec<_>>>();
            ChunkedArray::new(res_chunks?)
        }
        geoarrow::datatypes::NativeType::LineString(_, dimension) => {
            let res_chunks: std::result::Result<
                Vec<arrow::array::PrimitiveArray<Float64Type>>,
                Error,
            > = inner
                .chunks()
                .into_iter()
                .map(|chunk| {
                    let lines = chunk.to_line_string_array(dimension).handle_error()?;
                    let res = lines.euclidean_length();
                    Ok(res)
                })
                .collect::<Result<Vec<_>>>();
            ChunkedArray::new(res_chunks?)
        }
        geoarrow::datatypes::NativeType::MultiPoint(_, dimension) => {
            let res_chunks = inner
                .chunks()
                .into_iter()
                .map(|chunk| {
                    let lines = chunk.to_multi_point_array(dimension).handle_error()?;
                    let res = lines.euclidean_length();
                    Ok(res)
                })
                .collect::<Result<Vec<_>>>();
            ChunkedArray::new(res_chunks?)
        }
        geoarrow::datatypes::NativeType::MultiLineString(_, dimension) => {
            let res_chunks: std::result::Result<
                Vec<arrow::array::PrimitiveArray<Float64Type>>,
                Error,
            > = inner
                .chunks()
                .into_iter()
                .map(|chunk| {
                    let lines = chunk.to_multi_line_string_array(dimension).handle_error()?;
                    let res = lines.euclidean_length();
                    Ok(res)
                })
                .collect::<Result<Vec<_>>>();
            ChunkedArray::new(res_chunks?)
        }
        _ => {
            let res_chunks = inner
                .chunks()
                .into_iter()
                .map(|chunk| {
                    let res = Float64Array::new_null(chunk.len());
                    Ok(res)
                })
                .collect::<Result<Vec<_>>>();
            ChunkedArray::new(res_chunks?)
        }
    };

    Ok(PrimitiveChunks(res_array))
}

#[extendr]
pub fn length_vincenty_(x: GeoChunks) -> Result<PrimitiveChunks<Float64Type>> {
    let inner = x.0;
    let geo_type = inner.data_type();
    let res_array = match geo_type {
        geoarrow::datatypes::NativeType::Point(_, dimension) => {
            let res_chunks = inner
                .chunks()
                .into_iter()
                .map(|chunk| {
                    let lines = chunk.to_point_array(dimension).handle_error()?;
                    let res = lines.vincenty_length().handle_error()?;
                    Ok(res)
                })
                .collect::<Result<Vec<_>>>();
            ChunkedArray::new(res_chunks?)
        }
        geoarrow::datatypes::NativeType::LineString(_, dimension) => {
            let res_chunks: std::result::Result<
                Vec<arrow::array::PrimitiveArray<Float64Type>>,
                Error,
            > = inner
                .chunks()
                .into_iter()
                .map(|chunk| {
                    let lines = chunk.to_line_string_array(dimension).handle_error()?;
                    let res = lines.vincenty_length().handle_error()?;
                    Ok(res)
                })
                .collect::<Result<Vec<_>>>();
            ChunkedArray::new(res_chunks?)
        }
        geoarrow::datatypes::NativeType::MultiPoint(_, dimension) => {
            let res_chunks = inner
                .chunks()
                .into_iter()
                .map(|chunk| {
                    let lines = chunk.to_multi_point_array(dimension).handle_error()?;
                    let res = lines.vincenty_length().handle_error()?;
                    Ok(res)
                })
                .collect::<Result<Vec<_>>>();
            ChunkedArray::new(res_chunks?)
        }
        geoarrow::datatypes::NativeType::MultiLineString(_, dimension) => {
            let res_chunks: std::result::Result<
                Vec<arrow::array::PrimitiveArray<Float64Type>>,
                Error,
            > = inner
                .chunks()
                .into_iter()
                .map(|chunk| {
                    let lines = chunk.to_multi_line_string_array(dimension).handle_error()?;
                    let res = lines.vincenty_length().handle_error()?;
                    Ok(res)
                })
                .collect::<Result<Vec<_>>>();
            ChunkedArray::new(res_chunks?)
        }
        _ => {
            let res_chunks = inner
                .chunks()
                .into_iter()
                .map(|chunk| {
                    let res = Float64Array::new_null(chunk.len());
                    Ok(res)
                })
                .collect::<Result<Vec<_>>>();
            ChunkedArray::new(res_chunks?)
        }
    };

    Ok(PrimitiveChunks(res_array))
}

extendr_module! {
    mod length;
    fn length_haversine_;
    fn length_euclidean_;
    fn length_geodesic_;
    fn length_vincenty_;
}
