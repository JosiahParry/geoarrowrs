mod idx;
mod vw;
mod vw_preserve;

use arrow_extendr::IntoArrowRobj;
use extendr_api::prelude::*;
use geo::{LineString, MultiLineString, MultiPolygon, Polygon, Simplify};
use geo_traits::to_geo::{ToGeoLineString, ToGeoMultiLineString, ToGeoMultiPolygon, ToGeoPolygon};
use geoarrow::{
    array::{LineStringBuilder, MultiLineStringBuilder, MultiPolygonBuilder, PolygonBuilder},
    datatypes::{Dimension, LineStringType, MultiLineStringType, MultiPolygonType, PolygonType},
};
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor};

use crate::{
    as_linestring_chunks, as_multilinestring_chunks, as_multipolygon_chunks, as_polygon_chunks,
    try_float_array,
};

/// Simplify geometries using the Ramer-Douglas-Peucker algorithm
///
/// Reduces the number of points in each geometry by removing vertices that
/// deviate less than `epsilon` from the simplified path. Accepts linestrings,
/// multilinestrings, polygons, and multipolygons.
///
/// @param geometry a GeoArrow linestring, multilinestring, polygon, or multipolygon array
/// @param epsilon the simplification tolerance; length 1 or the same length as `geometry`
/// @returns a GeoArrow array of the same geometry type as the input
/// @export
/// @family simplify
/// @references [Simplify](https://docs.rs/geo/latest/geo/algorithm/simplify/trait.Simplify.html)
#[extendr]
fn ga_simplify(geometry: Robj, epsilon: Robj) -> extendr_api::Result<Robj> {
    let eps = try_float_array(epsilon, "epsilon")?;

    if let Ok(chunks) = as_linestring_chunks(geometry.clone()) {
        let n: usize = chunks.iter().map(|c| c.len()).sum();
        if eps.len() != 1 && eps.len() != n {
            return Err(Error::Other(format!(
                "`epsilon` must be length 1 or the same length as `geometry` ({}), got {}",
                n,
                eps.len()
            )));
        }
        let metadata = chunks[0].data_type().metadata().clone();
        let mut bldr = LineStringBuilder::new(LineStringType::new(Dimension::XY, metadata));
        for chunk in &chunks {
            for (geom, e) in chunk.iter().zip(eps.iter().cycle()) {
                if let (Some(Ok(g)), Some(e)) = (geom, e) {
                    bldr.push_line_string(Some(&g.to_line_string().simplify(e)))
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
        if eps.len() != 1 && eps.len() != n {
            return Err(Error::Other(format!(
                "`epsilon` must be length 1 or the same length as `geometry` ({}), got {}",
                n,
                eps.len()
            )));
        }
        let metadata = chunks[0].data_type().metadata().clone();
        let mut bldr =
            MultiLineStringBuilder::new(MultiLineStringType::new(Dimension::XY, metadata));
        for chunk in &chunks {
            for (geom, e) in chunk.iter().zip(eps.iter().cycle()) {
                if let (Some(Ok(g)), Some(e)) = (geom, e) {
                    bldr.push_multi_line_string(Some(&g.to_multi_line_string().simplify(e)))
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
        if eps.len() != 1 && eps.len() != n {
            return Err(Error::Other(format!(
                "`epsilon` must be length 1 or the same length as `geometry` ({}), got {}",
                n,
                eps.len()
            )));
        }
        let metadata = chunks[0].data_type().metadata().clone();
        let mut bldr = PolygonBuilder::new(PolygonType::new(Dimension::XY, metadata));
        for chunk in &chunks {
            for (geom, e) in chunk.iter().zip(eps.iter().cycle()) {
                if let (Some(Ok(g)), Some(e)) = (geom, e) {
                    bldr.push_polygon(Some(&g.to_polygon().simplify(e)))
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
        if eps.len() != 1 && eps.len() != n {
            return Err(Error::Other(format!(
                "`epsilon` must be length 1 or the same length as `geometry` ({}), got {}",
                n,
                eps.len()
            )));
        }
        let metadata = chunks[0].data_type().metadata().clone();
        let mut bldr = MultiPolygonBuilder::new(MultiPolygonType::new(Dimension::XY, metadata));
        for chunk in &chunks {
            for (geom, e) in chunk.iter().zip(eps.iter().cycle()) {
                if let (Some(Ok(g)), Some(e)) = (geom, e) {
                    bldr.push_multi_polygon(Some(&g.to_multi_polygon().simplify(e)))
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
        "Expected (multi)linestrings or (multi)polygons".to_string(),
    ))
}

extendr_module! {
    mod simplify;
    fn ga_simplify;
    use vw;
    use vw_preserve;
    use idx;
}
