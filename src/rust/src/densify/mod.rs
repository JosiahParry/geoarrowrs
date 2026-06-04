use crate::{
    as_linestring_chunks, as_multilinestring_chunks, as_multipolygon_chunks, as_polygon_chunks,
    try_float_array,
};
use arrow_extendr::IntoArrowRobj;
use extendr_api::prelude::*;
use geo::{
    Euclidean, Geodesic, Haversine, LineString, MultiLineString, MultiPolygon, Polygon, Rhumb,
    line_measures::Densifiable,
};
use geo_traits::to_geo::{ToGeoLineString, ToGeoMultiLineString, ToGeoMultiPolygon, ToGeoPolygon};
use geoarrow::{
    array::{LineStringBuilder, MultiLineStringBuilder, MultiPolygonBuilder, PolygonBuilder},
    datatypes::{Dimension, LineStringType, MultiLineStringType, MultiPolygonType, PolygonType},
};
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor};

/// Add intermediate points to geometries so no segment exceeds a maximum length
///
/// Densifies linestrings, multilinestrings, polygons, and multipolygons by
/// inserting additional points along each segment until no segment exceeds
/// `max_segment_length`. The metric determines how segment length is measured.
///
/// @param geometry a GeoArrow linestring, multilinestring, polygon, or multipolygon array
/// @param max_segment_length the maximum segment length; either length 1 or the same length as `geometry`
/// @param metric one of `"euclidean"`, `"haversine"`, `"geodesic"`, or `"rhumb"`
/// @returns a GeoArrow array of the same geometry type as the input
/// @export
/// @family densify
/// @references [Densifiable](https://docs.rs/geo/latest/geo/algorithm/line_measures/trait.Densifiable.html)
#[extendr]
fn densify(geometry: Robj, max_segment_length: Robj, metric: &str) -> extendr_api::Result<Robj> {
    let msl = try_float_array(max_segment_length, "max_segment_length")?;

    macro_rules! run_densify {
        ($metric_val:expr, $chunks:ident, $bldr:ident, $to_geo:ident, $push:ident, $null_ty:ty) => {
            for chunk in &$chunks {
                for (geom, msl_val) in chunk.iter().zip(msl.iter().cycle()) {
                    if let (Some(Ok(g)), Some(ms)) = (geom, msl_val) {
                        let value = g.$to_geo().densify(&$metric_val, ms);
                        $bldr
                            .$push(Some(&value))
                            .map_err(|e| Error::Other(e.to_string()))?;
                    } else {
                        $bldr
                            .$push(None::<&$null_ty>)
                            .map_err(|e| Error::Other(e.to_string()))?;
                    }
                }
            }
        };
    }

    macro_rules! dispatch_metrics {
        ($chunks:ident, $bldr:ident, $to_geo:ident, $push:ident, $null_ty:ty) => {{
            let n: usize = $chunks.iter().map(|c| c.len()).sum();
            if msl.len() != 1 && msl.len() != n {
                return Err(Error::Other(format!(
                    "`max_segment_length` must be length 1 or the same length as `geometry` ({}), got {}",
                    n,
                    msl.len()
                )));
            }
            match metric {
                "euclidean" => {
                    run_densify!(Euclidean, $chunks, $bldr, $to_geo, $push, $null_ty)
                }
                "haversine" => {
                    run_densify!(Haversine, $chunks, $bldr, $to_geo, $push, $null_ty)
                }
                "geodesic" => {
                    run_densify!(Geodesic, $chunks, $bldr, $to_geo, $push, $null_ty)
                }
                "rhumb" => {
                    run_densify!(Rhumb, $chunks, $bldr, $to_geo, $push, $null_ty)
                }
                _ => {
                    return Err(Error::Other(
                        "`metric` must be one of `euclidean`, `haversine`, `geodesic`, or `rhumb`"
                            .to_string(),
                    ))
                }
            }
        }}
    }

    if let Ok(chunks) = as_linestring_chunks(geometry.clone()) {
        let metadata = chunks[0].data_type().metadata().clone();
        let mut bldr = LineStringBuilder::new(LineStringType::new(Dimension::XY, metadata));
        dispatch_metrics!(
            chunks,
            bldr,
            to_line_string,
            push_line_string,
            LineString<f64>
        );
        return bldr.finish().into_arrow_robj();
    }

    if let Ok(chunks) = as_multilinestring_chunks(geometry.clone()) {
        let metadata = chunks[0].data_type().metadata().clone();
        let mut bldr =
            MultiLineStringBuilder::new(MultiLineStringType::new(Dimension::XY, metadata));
        dispatch_metrics!(
            chunks,
            bldr,
            to_multi_line_string,
            push_multi_line_string,
            MultiLineString<f64>
        );
        return bldr.finish().into_arrow_robj();
    }

    if let Ok(chunks) = as_polygon_chunks(geometry.clone()) {
        let metadata = chunks[0].data_type().metadata().clone();
        let mut bldr = PolygonBuilder::new(PolygonType::new(Dimension::XY, metadata));
        dispatch_metrics!(chunks, bldr, to_polygon, push_polygon, Polygon<f64>);
        return bldr.finish().into_arrow_robj();
    }

    if let Ok(chunks) = as_multipolygon_chunks(geometry) {
        let metadata = chunks[0].data_type().metadata().clone();
        let mut bldr = MultiPolygonBuilder::new(MultiPolygonType::new(Dimension::XY, metadata));
        dispatch_metrics!(
            chunks,
            bldr,
            to_multi_polygon,
            push_multi_polygon,
            MultiPolygon<f64>
        );
        return bldr.finish().into_arrow_robj();
    }

    Err(Error::Other(
        "Expected (multi)linestrings or (multi)polygons".to_string(),
    ))
}

extendr_module! {
    mod densify;
    fn densify;
}
