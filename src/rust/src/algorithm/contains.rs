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
                // SELF TO SELF
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
                (NativeType::MultiPoint(..), NativeType::MultiPoint(..)) => {
                    let lhs_array = lhs_dyn
                        .as_multi_point_opt()
                        .ok_or_else(|| Error::Other("Expected MultiPoint for lhs".to_string()))?;
                    let rhs_array = rhs_dyn
                        .as_multi_point_opt()
                        .ok_or_else(|| Error::Other("Expected MultiPoint for rhs".to_string()))?;
                    Ok(lhs_array.contains(rhs_array))
                }
                (NativeType::MultiLineString(..), NativeType::MultiLineString(..)) => {
                    let lhs_array = lhs_dyn.as_multi_line_string_opt().ok_or_else(|| {
                        Error::Other("Expected MultiLineString for lhs".to_string())
                    })?;
                    let rhs_array = rhs_dyn.as_multi_line_string_opt().ok_or_else(|| {
                        Error::Other("Expected MultiLineString for rhs".to_string())
                    })?;
                    Ok(lhs_array.contains(rhs_array))
                }
                (NativeType::MultiPolygon(..), NativeType::MultiPolygon(..)) => {
                    let lhs_array = lhs_dyn.as_multi_polygon_opt().ok_or_else(|| {
                        Error::Other("Expected MultiPolygonArray for lhs".to_string())
                    })?;
                    let rhs_array = rhs_dyn.as_multi_polygon_opt().ok_or_else(|| {
                        Error::Other("Expected MultiPolygonArray for rhs".to_string())
                    })?;
                    Ok(lhs_array.contains(rhs_array))
                }

                // Contains<GeometryArray>
                (NativeType::MultiPolygon(..), NativeType::Geometry(..)) => {
                    let lhs_array = lhs_dyn
                        .as_multi_polygon_opt()
                        .ok_or_else(|| Error::Other("Expected MultiPolygon for lhs".to_string()))?;
                    let rhs_array = rhs_dyn.as_geometry_opt().ok_or_else(|| {
                        Error::Other("Expected GeometryArray for rhs".to_string())
                    })?;
                    Ok(lhs_array.contains(rhs_array))
                }
                (NativeType::Polygon(..), NativeType::Geometry(..)) => {
                    let lhs_array = lhs_dyn.as_polygon();
                    let rhs_array = rhs_dyn.as_geometry_opt().ok_or_else(|| {
                        Error::Other("Expected GeometryArray for rhs".to_string())
                    })?;
                    Ok(lhs_array.contains(rhs_array))
                }
                (NativeType::MultiLineString(..), NativeType::Geometry(..)) => {
                    let lhs_array = lhs_dyn.as_multi_line_string();
                    let rhs_array = rhs_dyn.as_geometry_opt().ok_or_else(|| {
                        Error::Other("Expected GeometryArray for rhs".to_string())
                    })?;
                    Ok(lhs_array.contains(rhs_array))
                }
                (NativeType::LineString(..), NativeType::Geometry(..)) => {
                    let lhs_array = lhs_dyn.as_line_string();
                    let rhs_array = rhs_dyn.as_geometry_opt().ok_or_else(|| {
                        Error::Other("Expected GeometryArray for rhs".to_string())
                    })?;
                    Ok(lhs_array.contains(rhs_array))
                }
                (NativeType::MultiPoint(..), NativeType::Geometry(..)) => {
                    let lhs_array = lhs_dyn.as_multi_point();
                    let rhs_array = rhs_dyn.as_geometry_opt().ok_or_else(|| {
                        Error::Other("Expected GeometryArray for rhs".to_string())
                    })?;
                    Ok(lhs_array.contains(rhs_array))
                }
                (NativeType::Point(..), NativeType::Geometry(..)) => {
                    let lhs_array = lhs_dyn.as_point();
                    let rhs_array = rhs_dyn.as_geometry_opt().ok_or_else(|| {
                        Error::Other("Expected GeometryArray for rhs".to_string())
                    })?;
                    Ok(lhs_array.contains(rhs_array))
                }

                // Contains<LineStringArray>
                (NativeType::MultiPolygon(..), NativeType::LineString(..)) => {
                    let lhs_array = lhs_dyn
                        .as_multi_polygon_opt()
                        .ok_or_else(|| Error::Other("Expected MultiPolygon for lhs".to_string()))?;
                    let rhs_array = rhs_dyn
                        .as_line_string_opt()
                        .ok_or_else(|| Error::Other("Expected LineString for rhs".to_string()))?;
                    Ok(lhs_array.contains(rhs_array))
                }
                (NativeType::Polygon(..), NativeType::LineString(..)) => {
                    let lhs_array = lhs_dyn.as_polygon();
                    let rhs_array = rhs_dyn
                        .as_line_string_opt()
                        .ok_or_else(|| Error::Other("Expected LineString for rhs".to_string()))?;
                    Ok(lhs_array.contains(rhs_array))
                }
                (NativeType::MultiLineString(..), NativeType::LineString(..)) => {
                    let lhs_array = lhs_dyn.as_multi_line_string();
                    let rhs_array = rhs_dyn
                        .as_line_string_opt()
                        .ok_or_else(|| Error::Other("Expected LineString for rhs".to_string()))?;
                    Ok(lhs_array.contains(rhs_array))
                }
                (NativeType::MultiPoint(..), NativeType::LineString(..)) => {
                    let lhs_array = lhs_dyn.as_multi_point();
                    let rhs_array = rhs_dyn
                        .as_line_string_opt()
                        .ok_or_else(|| Error::Other("Expected LineString for rhs".to_string()))?;
                    Ok(lhs_array.contains(rhs_array))
                }
                (NativeType::Point(..), NativeType::LineString(..)) => {
                    let lhs_array = lhs_dyn.as_point();
                    let rhs_array = rhs_dyn
                        .as_line_string_opt()
                        .ok_or_else(|| Error::Other("Expected LineString for rhs".to_string()))?;
                    Ok(lhs_array.contains(rhs_array))
                }

                // Contains<MultiLineStringArray>
                (NativeType::MultiPolygon(..), NativeType::MultiLineString(..)) => {
                    let lhs_array = lhs_dyn
                        .as_multi_polygon_opt()
                        .ok_or_else(|| Error::Other("Expected MultiPolygon for lhs".to_string()))?;
                    let rhs_array = rhs_dyn.as_multi_line_string_opt().ok_or_else(|| {
                        Error::Other("Expected MultiLineString for rhs".to_string())
                    })?;
                    Ok(lhs_array.contains(rhs_array))
                }
                (NativeType::Polygon(..), NativeType::MultiLineString(..)) => {
                    let lhs_array = lhs_dyn.as_polygon();
                    let rhs_array = rhs_dyn.as_multi_line_string_opt().ok_or_else(|| {
                        Error::Other("Expected MultiLineString for rhs".to_string())
                    })?;
                    Ok(lhs_array.contains(rhs_array))
                }
                (NativeType::LineString(..), NativeType::MultiLineString(..)) => {
                    let lhs_array = lhs_dyn.as_line_string();
                    let rhs_array = rhs_dyn.as_multi_line_string_opt().ok_or_else(|| {
                        Error::Other("Expected MultiLineString for rhs".to_string())
                    })?;
                    Ok(lhs_array.contains(rhs_array))
                }
                (NativeType::MultiPoint(..), NativeType::MultiLineString(..)) => {
                    let lhs_array = lhs_dyn.as_multi_point();
                    let rhs_array = rhs_dyn.as_multi_line_string_opt().ok_or_else(|| {
                        Error::Other("Expected MultiLineString for rhs".to_string())
                    })?;
                    Ok(lhs_array.contains(rhs_array))
                }
                (NativeType::Point(..), NativeType::MultiLineString(..)) => {
                    let lhs_array = lhs_dyn.as_point();
                    let rhs_array = rhs_dyn.as_multi_line_string_opt().ok_or_else(|| {
                        Error::Other("Expected MultiLineString for rhs".to_string())
                    })?;
                    Ok(lhs_array.contains(rhs_array))
                }

                // Contains<MultiPointArray>
                (NativeType::MultiPolygon(..), NativeType::MultiPoint(..)) => {
                    let lhs_array = lhs_dyn
                        .as_multi_polygon_opt()
                        .ok_or_else(|| Error::Other("Expected MultiPolygon for lhs".to_string()))?;
                    let rhs_array = rhs_dyn
                        .as_multi_point_opt()
                        .ok_or_else(|| Error::Other("Expected MultiPoint for rhs".to_string()))?;
                    Ok(lhs_array.contains(rhs_array))
                }
                (NativeType::Polygon(..), NativeType::MultiPoint(..)) => {
                    let lhs_array = lhs_dyn.as_polygon();
                    let rhs_array = rhs_dyn
                        .as_multi_point_opt()
                        .ok_or_else(|| Error::Other("Expected MultiPoint for rhs".to_string()))?;
                    Ok(lhs_array.contains(rhs_array))
                }
                (NativeType::LineString(..), NativeType::MultiPoint(..)) => {
                    let lhs_array = lhs_dyn.as_line_string();
                    let rhs_array = rhs_dyn
                        .as_multi_point_opt()
                        .ok_or_else(|| Error::Other("Expected MultiPoint for rhs".to_string()))?;
                    Ok(lhs_array.contains(rhs_array))
                }
                (NativeType::MultiLineString(..), NativeType::MultiPoint(..)) => {
                    let lhs_array = lhs_dyn.as_multi_line_string();
                    let rhs_array = rhs_dyn
                        .as_multi_point_opt()
                        .ok_or_else(|| Error::Other("Expected MultiPoint for rhs".to_string()))?;
                    Ok(lhs_array.contains(rhs_array))
                }
                (NativeType::Point(..), NativeType::MultiPoint(..)) => {
                    let lhs_array = lhs_dyn.as_point();
                    let rhs_array = rhs_dyn
                        .as_multi_point_opt()
                        .ok_or_else(|| Error::Other("Expected MultiPoint for rhs".to_string()))?;
                    Ok(lhs_array.contains(rhs_array))
                }

                // Contains<MultiPolygonArray>
                (NativeType::Polygon(..), NativeType::MultiPolygon(..)) => {
                    let lhs_array = lhs_dyn.as_polygon();
                    let rhs_array = rhs_dyn
                        .as_multi_polygon_opt()
                        .ok_or_else(|| Error::Other("Expected MultiPolygon for rhs".to_string()))?;
                    Ok(lhs_array.contains(rhs_array))
                }
                (NativeType::LineString(..), NativeType::MultiPolygon(..)) => {
                    let lhs_array = lhs_dyn.as_line_string();
                    let rhs_array = rhs_dyn
                        .as_multi_polygon_opt()
                        .ok_or_else(|| Error::Other("Expected MultiPolygon for rhs".to_string()))?;
                    Ok(lhs_array.contains(rhs_array))
                }
                (NativeType::MultiLineString(..), NativeType::MultiPolygon(..)) => {
                    let lhs_array = lhs_dyn.as_multi_line_string();
                    let rhs_array = rhs_dyn
                        .as_multi_polygon_opt()
                        .ok_or_else(|| Error::Other("Expected MultiPolygon for rhs".to_string()))?;
                    Ok(lhs_array.contains(rhs_array))
                }
                (NativeType::Point(..), NativeType::MultiPolygon(..)) => {
                    let lhs_array = lhs_dyn.as_point();
                    let rhs_array = rhs_dyn
                        .as_multi_polygon_opt()
                        .ok_or_else(|| Error::Other("Expected MultiPolygon for rhs".to_string()))?;
                    Ok(lhs_array.contains(rhs_array))
                }

                // Contains<PointArray>
                (NativeType::MultiPolygon(..), NativeType::Point(..)) => {
                    let lhs_array = lhs_dyn
                        .as_multi_polygon_opt()
                        .ok_or_else(|| Error::Other("Expected MultiPolygon for lhs".to_string()))?;
                    let rhs_array = rhs_dyn
                        .as_point_opt()
                        .ok_or_else(|| Error::Other("Expected Point for rhs".to_string()))?;
                    Ok(lhs_array.contains(rhs_array))
                }
                (NativeType::Polygon(..), NativeType::Point(..)) => {
                    let lhs_array = lhs_dyn.as_polygon();
                    let rhs_array = rhs_dyn
                        .as_point_opt()
                        .ok_or_else(|| Error::Other("Expected Point for rhs".to_string()))?;
                    Ok(lhs_array.contains(rhs_array))
                }
                (NativeType::LineString(..), NativeType::Point(..)) => {
                    let lhs_array = lhs_dyn.as_line_string();
                    let rhs_array = rhs_dyn
                        .as_point_opt()
                        .ok_or_else(|| Error::Other("Expected Point for rhs".to_string()))?;
                    Ok(lhs_array.contains(rhs_array))
                }
                (NativeType::MultiLineString(..), NativeType::Point(..)) => {
                    let lhs_array = lhs_dyn.as_multi_line_string();
                    let rhs_array = rhs_dyn
                        .as_point_opt()
                        .ok_or_else(|| Error::Other("Expected Point for rhs".to_string()))?;
                    Ok(lhs_array.contains(rhs_array))
                }
                (NativeType::MultiPoint(..), NativeType::Point(..)) => {
                    let lhs_array = lhs_dyn
                        .as_multi_point_opt()
                        .ok_or_else(|| Error::Other("Expected MultiPoint for lhs".to_string()))?;
                    let rhs_array = rhs_dyn
                        .as_point_opt()
                        .ok_or_else(|| Error::Other("Expected Point for rhs".to_string()))?;
                    Ok(lhs_array.contains(rhs_array))
                }

                // Contains<PolygonArray>
                (NativeType::Point(..), NativeType::Polygon(..)) => {
                    let lhs_array = lhs_dyn.as_point();
                    let rhs_array = rhs_dyn
                        .as_polygon_opt()
                        .ok_or_else(|| Error::Other("Expected Polygon for rhs".to_string()))?;
                    Ok(lhs_array.contains(rhs_array))
                }
                (NativeType::MultiPoint(..), NativeType::Polygon(..)) => {
                    let lhs_array = lhs_dyn.as_multi_point();
                    let rhs_array = rhs_dyn
                        .as_polygon_opt()
                        .ok_or_else(|| Error::Other("Expected Polygon for rhs".to_string()))?;
                    Ok(lhs_array.contains(rhs_array))
                }
                (NativeType::LineString(..), NativeType::Polygon(..)) => {
                    let lhs_array = lhs_dyn.as_line_string();
                    let rhs_array = rhs_dyn
                        .as_polygon_opt()
                        .ok_or_else(|| Error::Other("Expected Polygon for rhs".to_string()))?;
                    Ok(lhs_array.contains(rhs_array))
                }
                (NativeType::MultiLineString(..), NativeType::Polygon(..)) => {
                    let lhs_array = lhs_dyn.as_multi_line_string();
                    let rhs_array = rhs_dyn
                        .as_polygon_opt()
                        .ok_or_else(|| Error::Other("Expected Polygon for rhs".to_string()))?;
                    Ok(lhs_array.contains(rhs_array))
                }
                (NativeType::MultiPolygon(..), NativeType::Polygon(..)) => {
                    let lhs_array = lhs_dyn.as_multi_polygon();
                    let rhs_array = rhs_dyn
                        .as_polygon_opt()
                        .ok_or_else(|| Error::Other("Expected Polygon for rhs".to_string()))?;
                    Ok(lhs_array.contains(rhs_array))
                }

                // Contains<RectArray>
                (NativeType::Point(..), NativeType::Rect(..)) => {
                    let lhs_array = lhs_dyn.as_point();
                    let rhs_array = rhs_dyn
                        .as_rect_opt()
                        .ok_or_else(|| Error::Other("Expected Rect for rhs".to_string()))?;
                    Ok(lhs_array.contains(rhs_array))
                }
                (NativeType::MultiPoint(..), NativeType::Rect(..)) => {
                    let lhs_array = lhs_dyn.as_multi_point();
                    let rhs_array = rhs_dyn
                        .as_rect_opt()
                        .ok_or_else(|| Error::Other("Expected Rect for rhs".to_string()))?;
                    Ok(lhs_array.contains(rhs_array))
                }
                (NativeType::LineString(..), NativeType::Rect(..)) => {
                    let lhs_array = lhs_dyn.as_line_string();
                    let rhs_array = rhs_dyn
                        .as_rect_opt()
                        .ok_or_else(|| Error::Other("Expected Rect for rhs".to_string()))?;
                    Ok(lhs_array.contains(rhs_array))
                }
                (NativeType::MultiLineString(..), NativeType::Rect(..)) => {
                    let lhs_array = lhs_dyn.as_multi_line_string();
                    let rhs_array = rhs_dyn
                        .as_rect_opt()
                        .ok_or_else(|| Error::Other("Expected Rect for rhs".to_string()))?;
                    Ok(lhs_array.contains(rhs_array))
                }
                (NativeType::Polygon(..), NativeType::Rect(..)) => {
                    let lhs_array = lhs_dyn.as_polygon();
                    let rhs_array = rhs_dyn
                        .as_rect_opt()
                        .ok_or_else(|| Error::Other("Expected Rect for rhs".to_string()))?;
                    Ok(lhs_array.contains(rhs_array))
                }
                (NativeType::MultiPolygon(..), NativeType::Rect(..)) => {
                    let lhs_array = lhs_dyn.as_multi_polygon();
                    let rhs_array = rhs_dyn
                        .as_rect_opt()
                        .ok_or_else(|| Error::Other("Expected Rect for rhs".to_string()))?;
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

// contains is the same as within but the arguments swapped sooo...to save myself...
#[extendr]
pub fn within_(x: GeoChunks, y: GeoChunks) -> Result<BooleanChunks> {
    contains_(y, x)
}

extendr_module! {
    mod contains;
    fn contains_;
    fn within_;
}
