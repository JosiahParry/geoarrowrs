use arrow_extendr::IntoArrowRobj;
use extendr_api::prelude::*;
use geo::{LineString, MultiLineString, MultiPoint, MultiPolygon, Point, Polygon};
use geo::{ToDegrees, ToRadians};
use geo_traits::to_geo::{
    ToGeoLineString, ToGeoMultiLineString, ToGeoMultiPoint, ToGeoMultiPolygon, ToGeoPoint,
    ToGeoPolygon,
};
use geoarrow::array::{
    LineStringBuilder, MultiLineStringBuilder, MultiPointBuilder, MultiPolygonBuilder,
    PointBuilder, PolygonBuilder,
};
use geoarrow::datatypes::{
    Dimension, LineStringType, MultiLineStringType, MultiPointType, MultiPolygonType, PointType,
    PolygonType,
};
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor};

use crate::{
    as_linestring_chunks, as_multilinestring_chunks, as_multipoint_chunks, as_multipolygon_chunks,
    as_point_chunks, as_polygon_chunks,
};

// both functions are the same walk with a different geo method, so the body is generated per operation
macro_rules! convert_angle {
    ($name:ident, $op:ident, $verb:literal) => {
        fn $name(geometry: Robj) -> extendr_api::Result<Robj> {
            if let Ok(chunks) = as_point_chunks(geometry.clone()) {
                let metadata = chunks[0].data_type().metadata().clone();
                let mut bldr = PointBuilder::new(PointType::new(Dimension::XY, metadata));
                for chunk in &chunks {
                    for geom in chunk.iter() {
                        // try_ rather than to_point, which panics on an empty point
                        match geom.and_then(|g| g.ok()).and_then(|g| g.try_to_point()) {
                            Some(p) => bldr.push_point(Some(&p.$op())),
                            None => bldr.push_point(None::<&Point<f64>>),
                        }
                    }
                }
                return bldr.finish().into_arrow_robj();
            }

            if let Ok(chunks) = as_multipoint_chunks(geometry.clone()) {
                let metadata = chunks[0].data_type().metadata().clone();
                let mut bldr = MultiPointBuilder::new(MultiPointType::new(Dimension::XY, metadata));
                for chunk in &chunks {
                    for geom in chunk.iter() {
                        match geom
                            .and_then(|g| g.ok())
                            .and_then(|g| g.try_to_multi_point())
                        {
                            Some(mp) => bldr.push_multi_point(Some(&mp.$op())),
                            None => bldr.push_multi_point(None::<&MultiPoint<f64>>),
                        }
                        .map_err(|e| Error::Other(e.to_string()))?;
                    }
                }
                return bldr.finish().into_arrow_robj();
            }

            if let Ok(chunks) = as_linestring_chunks(geometry.clone()) {
                let metadata = chunks[0].data_type().metadata().clone();
                let mut bldr = LineStringBuilder::new(LineStringType::new(Dimension::XY, metadata));
                for chunk in &chunks {
                    for geom in chunk.iter() {
                        match geom {
                            Some(Ok(g)) => bldr.push_line_string(Some(&g.to_line_string().$op())),
                            _ => bldr.push_line_string(None::<&LineString<f64>>),
                        }
                        .map_err(|e| Error::Other(e.to_string()))?;
                    }
                }
                return bldr.finish().into_arrow_robj();
            }

            if let Ok(chunks) = as_multilinestring_chunks(geometry.clone()) {
                let metadata = chunks[0].data_type().metadata().clone();
                let mut bldr =
                    MultiLineStringBuilder::new(MultiLineStringType::new(Dimension::XY, metadata));
                for chunk in &chunks {
                    for geom in chunk.iter() {
                        match geom {
                            Some(Ok(g)) => {
                                bldr.push_multi_line_string(Some(&g.to_multi_line_string().$op()))
                            }
                            _ => bldr.push_multi_line_string(None::<&MultiLineString<f64>>),
                        }
                        .map_err(|e| Error::Other(e.to_string()))?;
                    }
                }
                return bldr.finish().into_arrow_robj();
            }

            if let Ok(chunks) = as_polygon_chunks(geometry.clone()) {
                let metadata = chunks[0].data_type().metadata().clone();
                let mut bldr = PolygonBuilder::new(PolygonType::new(Dimension::XY, metadata));
                for chunk in &chunks {
                    for geom in chunk.iter() {
                        match geom {
                            Some(Ok(g)) => bldr.push_polygon(Some(&g.to_polygon().$op())),
                            _ => bldr.push_polygon(None::<&Polygon<f64>>),
                        }
                        .map_err(|e| Error::Other(e.to_string()))?;
                    }
                }
                return bldr.finish().into_arrow_robj();
            }

            if let Ok(chunks) = as_multipolygon_chunks(geometry) {
                let metadata = chunks[0].data_type().metadata().clone();
                let mut bldr =
                    MultiPolygonBuilder::new(MultiPolygonType::new(Dimension::XY, metadata));
                for chunk in &chunks {
                    for geom in chunk.iter() {
                        match geom {
                            Some(Ok(g)) => {
                                bldr.push_multi_polygon(Some(&g.to_multi_polygon().$op()))
                            }
                            _ => bldr.push_multi_polygon(None::<&MultiPolygon<f64>>),
                        }
                        .map_err(|e| Error::Other(e.to_string()))?;
                    }
                }
                return bldr.finish().into_arrow_robj();
            }

            Err(Error::Other(format!("Cannot {} this geometry type", $verb)))
        }
    };
}

/// Convert coordinates from radians to degrees
///
/// Multiplies every x and y coordinate by `180 / pi`. The geometry type of the
/// output matches the geometry type of the input.
///
/// @details
/// Only the x and y coordinates are converted. Z and M values are dropped,
/// since the conversion goes through a two dimensional representation.
///
/// @param geometry a GeoArrow geometry array
/// @returns a GeoArrow array of the same geometry type as the input
/// @export
/// @family convert
/// @references [ToDegrees](https://docs.rs/geo/latest/geo/algorithm/convert_angle_unit/trait.ToDegrees.html)
#[extendr]
fn to_degrees(geometry: Robj) -> extendr_api::Result<Robj> {
    to_degrees_impl(geometry)
}

/// Convert coordinates from degrees to radians
///
/// Multiplies every x and y coordinate by `pi / 180`. The geometry type of the
/// output matches the geometry type of the input.
///
/// @details
/// Only the x and y coordinates are converted. Z and M values are dropped,
/// since the conversion goes through a two dimensional representation.
///
/// @param geometry a GeoArrow geometry array
/// @returns a GeoArrow array of the same geometry type as the input
/// @export
/// @family convert
/// @references [ToRadians](https://docs.rs/geo/latest/geo/algorithm/convert_angle_unit/trait.ToRadians.html)
#[extendr]
fn to_radians(geometry: Robj) -> extendr_api::Result<Robj> {
    to_radians_impl(geometry)
}

convert_angle!(to_degrees_impl, to_degrees, "convert to degrees");
convert_angle!(to_radians_impl, to_radians, "convert to radians");

extendr_module! {
    mod convert;
    fn to_degrees;
    fn to_radians;
}
