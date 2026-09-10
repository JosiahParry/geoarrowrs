use arrow_extendr::IntoArrowRobj;
use extendr_api::prelude::*;
use geo::MultiLineString;
use geo::{LineStringSegmentize, LineStringSegmentizeHaversine};
use geo_traits::to_geo::ToGeoLineString;
use geoarrow::{
    array::MultiLineStringBuilder,
    datatypes::{Dimension, MultiLineStringType},
};
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor};

use crate::as_linestring_chunks;

/// Split linestrings into a given number of equal-length segments
///
/// Divides each linestring into `segment_count` segments of equal length,
/// returning a multilinestring. Returns `NA` if segmentation fails.
/// `ga_line_segmentize_haversine()` uses the Haversine formula for geographic
/// coordinates; `ga_line_segmentize()` uses planar Euclidean distance.
///
/// @param geometry a GeoArrow linestring array
/// @param segment_count the number of segments to split each linestring into; must be greater than 0
/// @returns a GeoArrow multilinestring array
/// @export
/// @rdname ga_line_segmentize
/// @family misc
/// @references [LineStringSegmentize](https://docs.rs/geo/latest/geo/algorithm/linestring_segment/trait.LineStringSegmentize.html)
#[extendr]
fn ga_line_segmentize(geometry: Robj, segment_count: i32) -> extendr_api::Result<Robj> {
    if segment_count <= 0 {
        return Err(Error::Other(
            "`segment_count` must be greater than 0".to_string(),
        ));
    }
    let n_segments = segment_count as usize;
    let chunks = as_linestring_chunks(geometry)?;
    let metadata = chunks[0].data_type().metadata().clone();
    let mut bldr = MultiLineStringBuilder::new(MultiLineStringType::new(Dimension::XY, metadata));

    for chunk in &chunks {
        for geom in chunk.iter() {
            if let Some(Ok(g)) = geom {
                match g.to_line_string().line_segmentize(n_segments) {
                    Some(result) => bldr
                        .push_multi_line_string(Some(&result))
                        .map_err(|e| Error::Other(e.to_string()))?,
                    None => bldr
                        .push_multi_line_string(None::<&MultiLineString<f64>>)
                        .map_err(|e| Error::Other(e.to_string()))?,
                }
            } else {
                bldr.push_multi_line_string(None::<&MultiLineString<f64>>)
                    .map_err(|e| Error::Other(e.to_string()))?;
            }
        }
    }

    bldr.finish().into_arrow_robj()
}

/// @export
/// @rdname ga_line_segmentize
/// @family misc
/// @references [LineStringSegmentizeHaversine](https://docs.rs/geo/latest/geo/algorithm/linestring_segment/trait.LineStringSegmentizeHaversine.html)
#[extendr]
fn ga_line_segmentize_haversine(geometry: Robj, segment_count: i32) -> extendr_api::Result<Robj> {
    if segment_count <= 0 {
        return Err(Error::Other(
            "`segment_count` must be greater than 0".to_string(),
        ));
    }
    let n_segments = segment_count as usize;
    let chunks = as_linestring_chunks(geometry)?;
    let metadata = chunks[0].data_type().metadata().clone();
    let mut bldr = MultiLineStringBuilder::new(MultiLineStringType::new(Dimension::XY, metadata));

    for chunk in &chunks {
        for geom in chunk.iter() {
            if let Some(Ok(g)) = geom {
                match g.to_line_string().line_segmentize_haversine(n_segments) {
                    Some(result) => bldr
                        .push_multi_line_string(Some(&result))
                        .map_err(|e| Error::Other(e.to_string()))?,
                    None => bldr
                        .push_multi_line_string(None::<&MultiLineString<f64>>)
                        .map_err(|e| Error::Other(e.to_string()))?,
                }
            } else {
                bldr.push_multi_line_string(None::<&MultiLineString<f64>>)
                    .map_err(|e| Error::Other(e.to_string()))?;
            }
        }
    }

    bldr.finish().into_arrow_robj()
}

extendr_module! {
    mod segmentize;
    fn ga_line_segmentize;
    fn ga_line_segmentize_haversine;
}
