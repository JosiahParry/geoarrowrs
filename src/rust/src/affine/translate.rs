use arrow::array::Float64Array;
use arrow_extendr::IntoArrowRobj;
use extendr_api::prelude::*;
use geo::{LineString, MultiLineString, MultiPolygon, Point, Polygon, Translate};
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

use crate::cast::CastGeoArrow;
use crate::threads::Threads;
use crate::{
    as_geo_geometries_par, as_geometry_chunks, as_linestring_chunks, as_multilinestring_chunks,
    as_multipolygon_chunks, as_point_chunks, as_polygon_chunks, check_recycle_len, try_float_array,
};

/// Translate geometries along the x and y axes
///
/// Shifts every coordinate of each geometry by `x_offset` and `y_offset`.
/// `ga_translate()` keeps the input's geometry type.
///
/// @details
/// `ga_translate()` accepts any geometry type, including multipoints,
/// geometry collections, and WKB. It returns a null for a null geometry or a
/// null offset.
///
/// @param geometry a GeoArrow geometry array
/// @param x_offset distance to shift along the x axis; length 1 or the same length as `geometry`
/// @param y_offset distance to shift along the y axis; length 1 or the same length as `geometry`
/// @returns a GeoArrow array of the same geometry type as the input
/// @export
/// @family affine
/// @references [Translate](https://docs.rs/geo/latest/geo/algorithm/translate/trait.Translate.html)
/// @examplesIf requireNamespace("sf", quietly = TRUE) && requireNamespace("geoarrow", quietly = TRUE)
/// pts <- ga_point(c(0, 1), c(0, 1))
///
/// # ga_translate() shifts each point by its own row's offsets
/// geoarrow::as_geoarrow_vctr(ga_translate(pts, c(10, 100), c(0, 5)))
#[extendr]
fn ga_translate(geometry: Robj, x_offset: Robj, y_offset: Robj) -> extendr_api::Result<Robj> {
    let xs = try_float_array(x_offset, "x_offset")?;
    let ys = try_float_array(y_offset, "y_offset")?;
    translate_by(geometry, &xs, &ys)
}

/// Shift each geometry by its row's offsets, keeping the geometry type and giving null for a null offset.
pub(super) fn translate_by(
    geometry: Robj,
    xs: &Float64Array,
    ys: &Float64Array,
) -> extendr_api::Result<Robj> {
    if let Ok(chunks) = as_point_chunks(geometry.clone()) {
        let n: usize = chunks.iter().map(|c| c.len()).sum();
        check_recycle_len(xs.len(), n, "x_offset")?;
        check_recycle_len(ys.len(), n, "y_offset")?;
        let metadata = chunks[0].data_type().metadata().clone();
        let mut bldr = PointBuilder::new(PointType::new(Dimension::XY, metadata));
        for chunk in &chunks {
            for ((geom, x), y) in chunk.iter().zip(xs.iter().cycle()).zip(ys.iter().cycle()) {
                if let (Some(Ok(g)), Some(x), Some(y)) = (geom, x, y) {
                    bldr.push_point(Some(&g.to_point().translate(x, y)));
                } else {
                    bldr.push_point(None::<&Point<f64>>);
                }
            }
        }
        return bldr.finish().into_arrow_robj();
    }

    if let Ok(chunks) = as_linestring_chunks(geometry.clone()) {
        let n: usize = chunks.iter().map(|c| c.len()).sum();
        check_recycle_len(xs.len(), n, "x_offset")?;
        check_recycle_len(ys.len(), n, "y_offset")?;
        let metadata = chunks[0].data_type().metadata().clone();
        let mut bldr = LineStringBuilder::new(LineStringType::new(Dimension::XY, metadata));
        for chunk in &chunks {
            for ((geom, x), y) in chunk.iter().zip(xs.iter().cycle()).zip(ys.iter().cycle()) {
                if let (Some(Ok(g)), Some(x), Some(y)) = (geom, x, y) {
                    bldr.push_line_string(Some(&g.to_line_string().translate(x, y)))
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
        check_recycle_len(xs.len(), n, "x_offset")?;
        check_recycle_len(ys.len(), n, "y_offset")?;
        let metadata = chunks[0].data_type().metadata().clone();
        let mut bldr =
            MultiLineStringBuilder::new(MultiLineStringType::new(Dimension::XY, metadata));
        for chunk in &chunks {
            for ((geom, x), y) in chunk.iter().zip(xs.iter().cycle()).zip(ys.iter().cycle()) {
                if let (Some(Ok(g)), Some(x), Some(y)) = (geom, x, y) {
                    bldr.push_multi_line_string(Some(&g.to_multi_line_string().translate(x, y)))
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
        check_recycle_len(xs.len(), n, "x_offset")?;
        check_recycle_len(ys.len(), n, "y_offset")?;
        let metadata = chunks[0].data_type().metadata().clone();
        let mut bldr = PolygonBuilder::new(PolygonType::new(Dimension::XY, metadata));
        for chunk in &chunks {
            for ((geom, x), y) in chunk.iter().zip(xs.iter().cycle()).zip(ys.iter().cycle()) {
                if let (Some(Ok(g)), Some(x), Some(y)) = (geom, x, y) {
                    bldr.push_polygon(Some(&g.to_polygon().translate(x, y)))
                        .map_err(|e| Error::Other(e.to_string()))?;
                } else {
                    bldr.push_polygon(None::<&Polygon<f64>>)
                        .map_err(|e| Error::Other(e.to_string()))?;
                }
            }
        }
        return bldr.finish().into_arrow_robj();
    }

    if let Ok(chunks) = as_multipolygon_chunks(geometry.clone()) {
        let n: usize = chunks.iter().map(|c| c.len()).sum();
        check_recycle_len(xs.len(), n, "x_offset")?;
        check_recycle_len(ys.len(), n, "y_offset")?;
        let metadata = chunks[0].data_type().metadata().clone();
        let mut bldr = MultiPolygonBuilder::new(MultiPolygonType::new(Dimension::XY, metadata));
        for chunk in &chunks {
            for ((geom, x), y) in chunk.iter().zip(xs.iter().cycle()).zip(ys.iter().cycle()) {
                if let (Some(Ok(g)), Some(x), Some(y)) = (geom, x, y) {
                    bldr.push_multi_polygon(Some(&g.to_multi_polygon().translate(x, y)))
                        .map_err(|e| Error::Other(e.to_string()))?;
                } else {
                    bldr.push_multi_polygon(None::<&MultiPolygon<f64>>)
                        .map_err(|e| Error::Other(e.to_string()))?;
                }
            }
        }
        return bldr.finish().into_arrow_robj();
    }

    // any other type, such as a multipoint, a collection, or WKB, goes through geo and casts back
    let chunks = as_geometry_chunks(geometry)?;
    let to_type = chunks[0].data_type();
    let geoms = as_geo_geometries_par(Threads::get(), &chunks)?;
    check_recycle_len(xs.len(), geoms.len(), "x_offset")?;
    check_recycle_len(ys.len(), geoms.len(), "y_offset")?;

    let moved = geoms
        .into_iter()
        .zip(xs.iter().cycle())
        .zip(ys.iter().cycle())
        .map(|((g, x), y)| match (g, x, y) {
            (Some(g), Some(x), Some(y)) => Some(g.translate(x, y)),
            _ => None,
        })
        .collect::<Vec<_>>();

    moved.cast_geoarrow(&to_type)?.into_arrow_robj()
}

extendr_module! {
    mod translate;
    fn ga_translate;
}
