use arrow_extendr::IntoArrowRobj;
use extendr_api::prelude::*;
use geo::{ConcaveHull, Polygon, algorithm::concave_hull::ConcaveHullOptions};
use geo_traits::to_geo::{
    ToGeoLineString, ToGeoMultiLineString, ToGeoMultiPoint, ToGeoMultiPolygon, ToGeoPolygon,
};
use geoarrow::{
    array::PolygonBuilder,
    datatypes::{Dimension, PolygonType},
};
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor};

use crate::{
    as_linestring_chunks, as_multilinestring_chunks, as_multipoint_chunks, as_multipolygon_chunks,
    as_polygon_chunks, check_recycle_len, try_float_array,
};

/// Compute the concave hull of geometries
///
/// Returns a polygon enclosing each geometry, tightened toward the geometry's
/// own shape. Smaller `concavity` values produce a tighter, more concave hull;
/// larger values approach the convex hull.
///
/// Accepts multipoints, linestrings, multilinestrings, polygons, and
/// multipolygons. `geo` does not define a concave hull for a single point.
///
/// @param geometry a GeoArrow multipoint, linestring, multilinestring, polygon, or multipolygon array
/// @param concavity the concavity coefficient; length 1 or the same length as `geometry`. `geo` uses 2 by default
/// @param length_threshold edges shorter than this are not considered for further refinement; length 1 or the same length as `geometry`. `geo` uses 0 by default
/// @returns a GeoArrow polygon array
/// @export
/// @family boundary
/// @references [ConcaveHull](https://docs.rs/geo/latest/geo/algorithm/concave_hull/trait.ConcaveHull.html)
#[extendr]
fn concave_hull(
    geometry: Robj,
    concavity: Robj,
    length_threshold: Robj,
) -> extendr_api::Result<Robj> {
    let cs = try_float_array(concavity, "concavity")?;
    let lts = try_float_array(length_threshold, "length_threshold")?;

    if let Ok(chunks) = as_multipoint_chunks(geometry.clone()) {
        let n: usize = chunks.iter().map(|c| c.len()).sum();
        check_recycle_len(cs.len(), n, "concavity")?;
        check_recycle_len(lts.len(), n, "length_threshold")?;
        let metadata = chunks[0].data_type().metadata().clone();
        let mut bldr = PolygonBuilder::new(PolygonType::new(Dimension::XY, metadata));
        for chunk in &chunks {
            for ((geom, c), lt) in chunk.iter().zip(cs.iter().cycle()).zip(lts.iter().cycle()) {
                if let (Some(Ok(g)), Some(c), Some(lt)) = (geom, c, lt) {
                    let opts = ConcaveHullOptions {
                        concavity: c,
                        length_threshold: lt,
                    };
                    bldr.push_polygon(Some(&g.to_multi_point().concave_hull_with_options(opts)))
                        .map_err(|e| Error::Other(e.to_string()))?;
                } else {
                    bldr.push_polygon(None::<&Polygon<f64>>)
                        .map_err(|e| Error::Other(e.to_string()))?;
                }
            }
        }
        return bldr.finish().into_arrow_robj();
    }

    if let Ok(chunks) = as_linestring_chunks(geometry.clone()) {
        let n: usize = chunks.iter().map(|c| c.len()).sum();
        check_recycle_len(cs.len(), n, "concavity")?;
        check_recycle_len(lts.len(), n, "length_threshold")?;
        let metadata = chunks[0].data_type().metadata().clone();
        let mut bldr = PolygonBuilder::new(PolygonType::new(Dimension::XY, metadata));
        for chunk in &chunks {
            for ((geom, c), lt) in chunk.iter().zip(cs.iter().cycle()).zip(lts.iter().cycle()) {
                if let (Some(Ok(g)), Some(c), Some(lt)) = (geom, c, lt) {
                    let opts = ConcaveHullOptions {
                        concavity: c,
                        length_threshold: lt,
                    };
                    bldr.push_polygon(Some(&g.to_line_string().concave_hull_with_options(opts)))
                        .map_err(|e| Error::Other(e.to_string()))?;
                } else {
                    bldr.push_polygon(None::<&Polygon<f64>>)
                        .map_err(|e| Error::Other(e.to_string()))?;
                }
            }
        }
        return bldr.finish().into_arrow_robj();
    }

    if let Ok(chunks) = as_multilinestring_chunks(geometry.clone()) {
        let n: usize = chunks.iter().map(|c| c.len()).sum();
        check_recycle_len(cs.len(), n, "concavity")?;
        check_recycle_len(lts.len(), n, "length_threshold")?;
        let metadata = chunks[0].data_type().metadata().clone();
        let mut bldr = PolygonBuilder::new(PolygonType::new(Dimension::XY, metadata));
        for chunk in &chunks {
            for ((geom, c), lt) in chunk.iter().zip(cs.iter().cycle()).zip(lts.iter().cycle()) {
                if let (Some(Ok(g)), Some(c), Some(lt)) = (geom, c, lt) {
                    let opts = ConcaveHullOptions {
                        concavity: c,
                        length_threshold: lt,
                    };
                    bldr.push_polygon(Some(
                        &g.to_multi_line_string().concave_hull_with_options(opts),
                    ))
                    .map_err(|e| Error::Other(e.to_string()))?;
                } else {
                    bldr.push_polygon(None::<&Polygon<f64>>)
                        .map_err(|e| Error::Other(e.to_string()))?;
                }
            }
        }
        return bldr.finish().into_arrow_robj();
    }

    if let Ok(chunks) = as_polygon_chunks(geometry.clone()) {
        let n: usize = chunks.iter().map(|c| c.len()).sum();
        check_recycle_len(cs.len(), n, "concavity")?;
        check_recycle_len(lts.len(), n, "length_threshold")?;
        let metadata = chunks[0].data_type().metadata().clone();
        let mut bldr = PolygonBuilder::new(PolygonType::new(Dimension::XY, metadata));
        for chunk in &chunks {
            for ((geom, c), lt) in chunk.iter().zip(cs.iter().cycle()).zip(lts.iter().cycle()) {
                if let (Some(Ok(g)), Some(c), Some(lt)) = (geom, c, lt) {
                    let opts = ConcaveHullOptions {
                        concavity: c,
                        length_threshold: lt,
                    };
                    bldr.push_polygon(Some(&g.to_polygon().concave_hull_with_options(opts)))
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
        check_recycle_len(cs.len(), n, "concavity")?;
        check_recycle_len(lts.len(), n, "length_threshold")?;
        let metadata = chunks[0].data_type().metadata().clone();
        let mut bldr = PolygonBuilder::new(PolygonType::new(Dimension::XY, metadata));
        for chunk in &chunks {
            for ((geom, c), lt) in chunk.iter().zip(cs.iter().cycle()).zip(lts.iter().cycle()) {
                if let (Some(Ok(g)), Some(c), Some(lt)) = (geom, c, lt) {
                    let opts = ConcaveHullOptions {
                        concavity: c,
                        length_threshold: lt,
                    };
                    bldr.push_polygon(Some(&g.to_multi_polygon().concave_hull_with_options(opts)))
                        .map_err(|e| Error::Other(e.to_string()))?;
                } else {
                    bldr.push_polygon(None::<&Polygon<f64>>)
                        .map_err(|e| Error::Other(e.to_string()))?;
                }
            }
        }
        return bldr.finish().into_arrow_robj();
    }

    Err(Error::Other(
        "Expected multipoints, (multi)linestrings, or (multi)polygons".to_string(),
    ))
}

extendr_module! {
    mod concave_hull;
    fn concave_hull;
}
