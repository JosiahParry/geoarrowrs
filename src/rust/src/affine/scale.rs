use arrow_extendr::IntoArrowRobj;
use extendr_api::prelude::*;
use geo::{LineString, MultiLineString, MultiPolygon, Point, Polygon, Scale};
use geo_traits::to_geo::{
    ToGeoLineString, ToGeoMultiLineString, ToGeoMultiPolygon, ToGeoPoint, ToGeoPolygon,
};
use geoarrow::{
    array::{
        LineStringBuilder, MultiLineStringBuilder, MultiPolygonBuilder, PointBuilder,
        PolygonBuilder,
    },
    datatypes::{
        Dimension, LineStringType, MultiLineStringType, MultiPolygonType, PointType, PolygonType,
    },
};
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor};

use crate::{
    as_linestring_chunks, as_multilinestring_chunks, as_multipolygon_chunks, as_point_chunks,
    as_polygon_chunks, check_recycle_len, try_float_array,
};

/// Scale geometries independently in x and y about their centroid
///
/// Scales each geometry by `x_factor` along the x axis and `y_factor` along the
/// y axis, about the geometry's own centroid. The geometry type of the output
/// matches the geometry type of the input.
///
/// @param geometry a GeoArrow point, linestring, multilinestring, polygon, or multipolygon array
/// @param x_factor scaling factor along the x axis; length 1 or the same length as `geometry`
/// @param y_factor scaling factor along the y axis; length 1 or the same length as `geometry`
/// @returns a GeoArrow array of the same geometry type as the input
/// @export
/// @family affine
/// @references [Scale](https://docs.rs/geo/latest/geo/algorithm/scale/trait.Scale.html)
#[extendr]
fn ga_scale_xy(geometry: Robj, x_factor: Robj, y_factor: Robj) -> extendr_api::Result<Robj> {
    let xs = try_float_array(x_factor, "x_factor")?;
    let ys = try_float_array(y_factor, "y_factor")?;

    if let Ok(chunks) = as_point_chunks(geometry.clone()) {
        let n: usize = chunks.iter().map(|c| c.len()).sum();
        check_recycle_len(xs.len(), n, "x_factor")?;
        check_recycle_len(ys.len(), n, "y_factor")?;
        let metadata = chunks[0].data_type().metadata().clone();
        let mut bldr = PointBuilder::new(PointType::new(Dimension::XY, metadata));
        for chunk in &chunks {
            for ((geom, x), y) in chunk.iter().zip(xs.iter().cycle()).zip(ys.iter().cycle()) {
                if let (Some(Ok(g)), Some(x), Some(y)) = (geom, x, y) {
                    bldr.push_point(Some(&g.to_point().scale_xy(x, y)));
                } else {
                    bldr.push_point(None::<&Point<f64>>);
                }
            }
        }
        return bldr.finish().into_arrow_robj();
    }

    if let Ok(chunks) = as_linestring_chunks(geometry.clone()) {
        let n: usize = chunks.iter().map(|c| c.len()).sum();
        check_recycle_len(xs.len(), n, "x_factor")?;
        check_recycle_len(ys.len(), n, "y_factor")?;
        let metadata = chunks[0].data_type().metadata().clone();
        let mut bldr = LineStringBuilder::new(LineStringType::new(Dimension::XY, metadata));
        for chunk in &chunks {
            for ((geom, x), y) in chunk.iter().zip(xs.iter().cycle()).zip(ys.iter().cycle()) {
                if let (Some(Ok(g)), Some(x), Some(y)) = (geom, x, y) {
                    bldr.push_line_string(Some(&g.to_line_string().scale_xy(x, y)))
                        .map_err(|e| Error::Other(e.to_string()))?;
                } else {
                    bldr.push_line_string(None::<&LineString<f64>>)
                        .map_err(|e| Error::Other(e.to_string()))?;
                }
            }
        }
        return bldr.finish().into_arrow_robj();
    }

    if let Ok(chunks) = as_multilinestring_chunks(geometry.clone()) {
        let n: usize = chunks.iter().map(|c| c.len()).sum();
        check_recycle_len(xs.len(), n, "x_factor")?;
        check_recycle_len(ys.len(), n, "y_factor")?;
        let metadata = chunks[0].data_type().metadata().clone();
        let mut bldr =
            MultiLineStringBuilder::new(MultiLineStringType::new(Dimension::XY, metadata));
        for chunk in &chunks {
            for ((geom, x), y) in chunk.iter().zip(xs.iter().cycle()).zip(ys.iter().cycle()) {
                if let (Some(Ok(g)), Some(x), Some(y)) = (geom, x, y) {
                    bldr.push_multi_line_string(Some(&g.to_multi_line_string().scale_xy(x, y)))
                        .map_err(|e| Error::Other(e.to_string()))?;
                } else {
                    bldr.push_multi_line_string(None::<&MultiLineString<f64>>)
                        .map_err(|e| Error::Other(e.to_string()))?;
                }
            }
        }
        return bldr.finish().into_arrow_robj();
    }

    if let Ok(chunks) = as_polygon_chunks(geometry.clone()) {
        let n: usize = chunks.iter().map(|c| c.len()).sum();
        check_recycle_len(xs.len(), n, "x_factor")?;
        check_recycle_len(ys.len(), n, "y_factor")?;
        let metadata = chunks[0].data_type().metadata().clone();
        let mut bldr = PolygonBuilder::new(PolygonType::new(Dimension::XY, metadata));
        for chunk in &chunks {
            for ((geom, x), y) in chunk.iter().zip(xs.iter().cycle()).zip(ys.iter().cycle()) {
                if let (Some(Ok(g)), Some(x), Some(y)) = (geom, x, y) {
                    bldr.push_polygon(Some(&g.to_polygon().scale_xy(x, y)))
                        .map_err(|e| Error::Other(e.to_string()))?;
                } else {
                    bldr.push_polygon(None::<&Polygon<f64>>)
                        .map_err(|e| Error::Other(e.to_string()))?;
                }
            }
        }
        return bldr.finish().into_arrow_robj();
    }

    if let Ok(chunks) = as_multipolygon_chunks(geometry) {
        let n: usize = chunks.iter().map(|c| c.len()).sum();
        check_recycle_len(xs.len(), n, "x_factor")?;
        check_recycle_len(ys.len(), n, "y_factor")?;
        let metadata = chunks[0].data_type().metadata().clone();
        let mut bldr = MultiPolygonBuilder::new(MultiPolygonType::new(Dimension::XY, metadata));
        for chunk in &chunks {
            for ((geom, x), y) in chunk.iter().zip(xs.iter().cycle()).zip(ys.iter().cycle()) {
                if let (Some(Ok(g)), Some(x), Some(y)) = (geom, x, y) {
                    bldr.push_multi_polygon(Some(&g.to_multi_polygon().scale_xy(x, y)))
                        .map_err(|e| Error::Other(e.to_string()))?;
                } else {
                    bldr.push_multi_polygon(None::<&MultiPolygon<f64>>)
                        .map_err(|e| Error::Other(e.to_string()))?;
                }
            }
        }
        return bldr.finish().into_arrow_robj();
    }

    Err(Error::Other(
        "Expected points, (multi)linestrings, or (multi)polygons".to_string(),
    ))
}

extendr_module! {
    mod scale;
    fn ga_scale_xy;
}
