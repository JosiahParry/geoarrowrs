use arrow_extendr::IntoArrowRobj;
use extendr_api::prelude::*;
use geo::{LineString, MultiLineString, MultiPolygon, Point, Polygon, Rotate};
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

/// Rotate geometries around their centroid
///
/// Rotates each geometry counter-clockwise by `degrees` about its own centroid.
/// The geometry type of the output matches the geometry type of the input.
///
/// @param geometry a GeoArrow point, linestring, multilinestring, polygon, or multipolygon array
/// @param degrees angle of rotation; length 1 or the same length as `geometry`
/// @returns a GeoArrow array of the same geometry type as the input
/// @export
/// @family affine
/// @references [Rotate](https://docs.rs/geo/latest/geo/algorithm/rotate/trait.Rotate.html)
/// @examplesIf requireNamespace("sf", quietly = TRUE) && requireNamespace("geoarrow", quietly = TRUE)
/// # a right triangle, whose centroid is not its bounding box center
/// tri <- geoarrow::as_geoarrow_array(sf::st_sfc(
///   sf::st_polygon(list(rbind(c(0, 0), c(4, 0), c(0, 3), c(0, 0))))
/// ))
///
/// geoarrow::as_geoarrow_vctr(ga_rotate_around_centroid(tri, 90))
#[extendr]
fn ga_rotate_around_centroid(geometry: Robj, degrees: Robj) -> extendr_api::Result<Robj> {
    let ds = try_float_array(degrees, "degrees")?;

    if let Ok(chunks) = as_point_chunks(geometry.clone()) {
        let n: usize = chunks.iter().map(|c| c.len()).sum();
        check_recycle_len(ds.len(), n, "degrees")?;
        let metadata = chunks[0].data_type().metadata().clone();
        let mut bldr = PointBuilder::new(PointType::new(Dimension::XY, metadata));
        for chunk in &chunks {
            for (geom, d) in chunk.iter().zip(ds.iter().cycle()) {
                if let (Some(Ok(g)), Some(d)) = (geom, d) {
                    bldr.push_point(Some(&g.to_point().rotate_around_centroid(d)));
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
                    bldr.push_line_string(Some(&g.to_line_string().rotate_around_centroid(d)))
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
                    bldr.push_multi_line_string(Some(
                        &g.to_multi_line_string().rotate_around_centroid(d),
                    ))
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
                    bldr.push_polygon(Some(&g.to_polygon().rotate_around_centroid(d)))
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
                    bldr.push_multi_polygon(Some(&g.to_multi_polygon().rotate_around_centroid(d)))
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

/// Rotate around the bounding box center
///
/// Rotates each geometry counter-clockwise by `degrees` about the center of its
/// own axis-aligned bounding rectangle. The geometry type of the output matches
/// the geometry type of the input.
///
/// @param geometry a GeoArrow point, linestring, multilinestring, polygon, or multipolygon array
/// @param degrees angle of rotation; length 1 or the same length as `geometry`
/// @returns a GeoArrow array of the same geometry type as the input
/// @export
/// @family affine
/// @references [Rotate](https://docs.rs/geo/latest/geo/algorithm/rotate/trait.Rotate.html)
/// @examplesIf requireNamespace("sf", quietly = TRUE) && requireNamespace("geoarrow", quietly = TRUE)
/// # the same triangle, turned about its bounding box center instead
/// tri <- geoarrow::as_geoarrow_array(sf::st_sfc(
///   sf::st_polygon(list(rbind(c(0, 0), c(4, 0), c(0, 3), c(0, 0))))
/// ))
///
/// geoarrow::as_geoarrow_vctr(ga_rotate_around_center(tri, 90))
#[extendr]
fn ga_rotate_around_center(geometry: Robj, degrees: Robj) -> extendr_api::Result<Robj> {
    let ds = try_float_array(degrees, "degrees")?;

    if let Ok(chunks) = as_point_chunks(geometry.clone()) {
        let n: usize = chunks.iter().map(|c| c.len()).sum();
        check_recycle_len(ds.len(), n, "degrees")?;
        let metadata = chunks[0].data_type().metadata().clone();
        let mut bldr = PointBuilder::new(PointType::new(Dimension::XY, metadata));
        for chunk in &chunks {
            for (geom, d) in chunk.iter().zip(ds.iter().cycle()) {
                if let (Some(Ok(g)), Some(d)) = (geom, d) {
                    bldr.push_point(Some(&g.to_point().rotate_around_center(d)));
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
                    bldr.push_line_string(Some(&g.to_line_string().rotate_around_center(d)))
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
                    bldr.push_multi_line_string(Some(
                        &g.to_multi_line_string().rotate_around_center(d),
                    ))
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
                    bldr.push_polygon(Some(&g.to_polygon().rotate_around_center(d)))
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
                    bldr.push_multi_polygon(Some(&g.to_multi_polygon().rotate_around_center(d)))
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
    mod rotate;
    fn ga_rotate_around_centroid;
    fn ga_rotate_around_center;
}
