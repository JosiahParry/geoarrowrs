use arrow_extendr::IntoArrowRobj;
use extendr_api::prelude::*;
use geo::{LineString, MultiLineString, MultiPolygon, Point, Polygon, Skew};
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

/// Skew geometries uniformly about their centroid
///
/// Shears each geometry by `degrees` along both the x and y dimensions, about
/// the geometry's own centroid. The geometry type of the output matches the
/// geometry type of the input.
///
/// @param geometry a GeoArrow point, linestring, multilinestring, polygon, or multipolygon array
/// @param degrees the shear angle; length 1 or the same length as `geometry`
/// @returns a GeoArrow array of the same geometry type as the input
/// @export
/// @family affine
/// @references [Skew](https://docs.rs/geo/latest/geo/algorithm/skew/trait.Skew.html)
#[extendr]
fn ga_skew(geometry: Robj, degrees: Robj) -> extendr_api::Result<Robj> {
    let ds = try_float_array(degrees, "degrees")?;

    if let Ok(chunks) = as_point_chunks(geometry.clone()) {
        let n: usize = chunks.iter().map(|c| c.len()).sum();
        check_recycle_len(ds.len(), n, "degrees")?;
        let metadata = chunks[0].data_type().metadata().clone();
        let mut bldr = PointBuilder::new(PointType::new(Dimension::XY, metadata));
        for chunk in &chunks {
            for (geom, d) in chunk.iter().zip(ds.iter().cycle()) {
                if let (Some(Ok(g)), Some(d)) = (geom, d) {
                    bldr.push_point(Some(&g.to_point().skew(d)));
                } else {
                    bldr.push_point(None::<&Point<f64>>);
                }
            }
        }
        return bldr.finish().into_arrow_robj();
    }

    if let Ok(chunks) = as_linestring_chunks(geometry.clone()) {
        let n: usize = chunks.iter().map(|c| c.len()).sum();
        check_recycle_len(ds.len(), n, "degrees")?;
        let metadata = chunks[0].data_type().metadata().clone();
        let mut bldr = LineStringBuilder::new(LineStringType::new(Dimension::XY, metadata));
        for chunk in &chunks {
            for (geom, d) in chunk.iter().zip(ds.iter().cycle()) {
                if let (Some(Ok(g)), Some(d)) = (geom, d) {
                    bldr.push_line_string(Some(&g.to_line_string().skew(d)))
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
        check_recycle_len(ds.len(), n, "degrees")?;
        let metadata = chunks[0].data_type().metadata().clone();
        let mut bldr =
            MultiLineStringBuilder::new(MultiLineStringType::new(Dimension::XY, metadata));
        for chunk in &chunks {
            for (geom, d) in chunk.iter().zip(ds.iter().cycle()) {
                if let (Some(Ok(g)), Some(d)) = (geom, d) {
                    bldr.push_multi_line_string(Some(&g.to_multi_line_string().skew(d)))
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
        check_recycle_len(ds.len(), n, "degrees")?;
        let metadata = chunks[0].data_type().metadata().clone();
        let mut bldr = PolygonBuilder::new(PolygonType::new(Dimension::XY, metadata));
        for chunk in &chunks {
            for (geom, d) in chunk.iter().zip(ds.iter().cycle()) {
                if let (Some(Ok(g)), Some(d)) = (geom, d) {
                    bldr.push_polygon(Some(&g.to_polygon().skew(d)))
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
        check_recycle_len(ds.len(), n, "degrees")?;
        let metadata = chunks[0].data_type().metadata().clone();
        let mut bldr = MultiPolygonBuilder::new(MultiPolygonType::new(Dimension::XY, metadata));
        for chunk in &chunks {
            for (geom, d) in chunk.iter().zip(ds.iter().cycle()) {
                if let (Some(Ok(g)), Some(d)) = (geom, d) {
                    bldr.push_multi_polygon(Some(&g.to_multi_polygon().skew(d)))
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

/// Skew geometries independently in x and y about their centroid
///
/// Shears each geometry by `degrees_x` along the x dimension and `degrees_y`
/// along the y dimension, about the geometry's own centroid. The geometry type
/// of the output matches the geometry type of the input.
///
/// @param geometry a GeoArrow point, linestring, multilinestring, polygon, or multipolygon array
/// @param degrees_x shear angle along the x dimension; length 1 or the same length as `geometry`
/// @param degrees_y shear angle along the y dimension; length 1 or the same length as `geometry`
/// @returns a GeoArrow array of the same geometry type as the input
/// @export
/// @family affine
/// @references [Skew](https://docs.rs/geo/latest/geo/algorithm/skew/trait.Skew.html)
#[extendr]
fn ga_skew_xy(geometry: Robj, degrees_x: Robj, degrees_y: Robj) -> extendr_api::Result<Robj> {
    let xs = try_float_array(degrees_x, "degrees_x")?;
    let ys = try_float_array(degrees_y, "degrees_y")?;

    if let Ok(chunks) = as_point_chunks(geometry.clone()) {
        let n: usize = chunks.iter().map(|c| c.len()).sum();
        check_recycle_len(xs.len(), n, "degrees_x")?;
        check_recycle_len(ys.len(), n, "degrees_y")?;
        let metadata = chunks[0].data_type().metadata().clone();
        let mut bldr = PointBuilder::new(PointType::new(Dimension::XY, metadata));
        for chunk in &chunks {
            for ((geom, x), y) in chunk.iter().zip(xs.iter().cycle()).zip(ys.iter().cycle()) {
                if let (Some(Ok(g)), Some(x), Some(y)) = (geom, x, y) {
                    bldr.push_point(Some(&g.to_point().skew_xy(x, y)));
                } else {
                    bldr.push_point(None::<&Point<f64>>);
                }
            }
        }
        return bldr.finish().into_arrow_robj();
    }

    if let Ok(chunks) = as_linestring_chunks(geometry.clone()) {
        let n: usize = chunks.iter().map(|c| c.len()).sum();
        check_recycle_len(xs.len(), n, "degrees_x")?;
        check_recycle_len(ys.len(), n, "degrees_y")?;
        let metadata = chunks[0].data_type().metadata().clone();
        let mut bldr = LineStringBuilder::new(LineStringType::new(Dimension::XY, metadata));
        for chunk in &chunks {
            for ((geom, x), y) in chunk.iter().zip(xs.iter().cycle()).zip(ys.iter().cycle()) {
                if let (Some(Ok(g)), Some(x), Some(y)) = (geom, x, y) {
                    bldr.push_line_string(Some(&g.to_line_string().skew_xy(x, y)))
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
        check_recycle_len(xs.len(), n, "degrees_x")?;
        check_recycle_len(ys.len(), n, "degrees_y")?;
        let metadata = chunks[0].data_type().metadata().clone();
        let mut bldr =
            MultiLineStringBuilder::new(MultiLineStringType::new(Dimension::XY, metadata));
        for chunk in &chunks {
            for ((geom, x), y) in chunk.iter().zip(xs.iter().cycle()).zip(ys.iter().cycle()) {
                if let (Some(Ok(g)), Some(x), Some(y)) = (geom, x, y) {
                    bldr.push_multi_line_string(Some(&g.to_multi_line_string().skew_xy(x, y)))
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
        check_recycle_len(xs.len(), n, "degrees_x")?;
        check_recycle_len(ys.len(), n, "degrees_y")?;
        let metadata = chunks[0].data_type().metadata().clone();
        let mut bldr = PolygonBuilder::new(PolygonType::new(Dimension::XY, metadata));
        for chunk in &chunks {
            for ((geom, x), y) in chunk.iter().zip(xs.iter().cycle()).zip(ys.iter().cycle()) {
                if let (Some(Ok(g)), Some(x), Some(y)) = (geom, x, y) {
                    bldr.push_polygon(Some(&g.to_polygon().skew_xy(x, y)))
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
        check_recycle_len(xs.len(), n, "degrees_x")?;
        check_recycle_len(ys.len(), n, "degrees_y")?;
        let metadata = chunks[0].data_type().metadata().clone();
        let mut bldr = MultiPolygonBuilder::new(MultiPolygonType::new(Dimension::XY, metadata));
        for chunk in &chunks {
            for ((geom, x), y) in chunk.iter().zip(xs.iter().cycle()).zip(ys.iter().cycle()) {
                if let (Some(Ok(g)), Some(x), Some(y)) = (geom, x, y) {
                    bldr.push_multi_polygon(Some(&g.to_multi_polygon().skew_xy(x, y)))
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
    mod skew;
    fn ga_skew;
    fn ga_skew_xy;
}
