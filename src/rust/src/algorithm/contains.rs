use crate::{
    ffi::{BooleanChunks, GeoChunks},
    HandleError,
};
use extendr_api::prelude::*;
use geoarrow::algorithm::geo::Contains;
use geoarrow::array::AsNativeArray;
use geoarrow::chunked_array::ChunkedArray;
use geoarrow::datatypes::NativeType;
use geoarrow::NativeArray;

#[extendr]
pub fn contains_(x: GeoChunks, y: GeoChunks) -> Result<BooleanChunks> {
    let lhs = x.0;
    let rhs = y.0;

    let ldt = lhs.data_type();
    let rdt = rhs.data_type();

    let results = lhs
        .chunks()
        .into_iter()
        .zip(rhs.chunks())
        .map(|(lhsi, rhsi)| {
            let lhs_dyn = lhsi.as_ref();
            let rhs_dyn = rhsi.as_ref();

            let resi = match (ldt, rdt) {
                (NativeType::Point(..), NativeType::Point(..)) => {
                    let lhs_array = lhs_dyn
                        .as_point_opt()
                        .ok_or_else(|| Error::Other("Expected PointArray for lhs".to_string()))?;
                    let rhs_array = rhs_dyn
                        .as_point_opt()
                        .ok_or_else(|| Error::Other("Expected PointArray for rhs".to_string()))?;
                    Ok(lhs_array.contains(rhs_array))
                }
                (NativeType::LineString(..), NativeType::LineString(..)) => {
                    let lhs_array = lhs_dyn.as_line_string_opt().ok_or_else(|| {
                        Error::Other("Expected LineStringArray for lhs".to_string())
                    })?;
                    let rhs_array = rhs_dyn.as_line_string_opt().ok_or_else(|| {
                        Error::Other("Expected LineStringArray for rhs".to_string())
                    })?;
                    Ok(lhs_array.contains(rhs_array))
                }
                (NativeType::Polygon(..), NativeType::Polygon(..)) => {
                    let lhs_array = lhs_dyn
                        .as_polygon_opt()
                        .ok_or_else(|| Error::Other("Expected PolygonArray for lhs".to_string()))?;
                    let rhs_array = rhs_dyn
                        .as_polygon_opt()
                        .ok_or_else(|| Error::Other("Expected PolygonArray for rhs".to_string()))?;
                    Ok(lhs_array.contains(rhs_array))
                }
                // Add more match arms for the other compatible types...
                _ => Err(Error::Other("Incompatible geometry types".to_string())),
            };
            resi
        })
        .collect::<Result<Vec<_>>>()
        .handle_error()?;

    let res = ChunkedArray::new(results);
    Ok(BooleanChunks(res))
}

extendr_module! {
    mod contains;
    fn contains_;
}
