use arrow_extendr::IntoArrowRobj;
use extendr_api::prelude::*;
use geo::{LineString, MultiLineString, MultiPolygon, Polygon, RemoveRepeatedPoints};
use geo_traits::to_geo::{ToGeoLineString, ToGeoMultiLineString, ToGeoMultiPolygon, ToGeoPolygon};
use geoarrow::{
    array::{LineStringBuilder, MultiLineStringBuilder, MultiPolygonBuilder, PolygonBuilder},
    datatypes::{Dimension, LineStringType, MultiLineStringType, MultiPolygonType, PolygonType},
};
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor};

use crate::{
    as_linestring_chunks, as_multilinestring_chunks, as_multipolygon_chunks, as_polygon_chunks,
};

#[extendr]
fn remove_repeated_points(geometry: Robj) -> extendr_api::Result<Robj> {
    if let Ok(chunks) = as_linestring_chunks(geometry.clone()) {
        let metadata = chunks[0].data_type().metadata().clone();
        let mut bldr = LineStringBuilder::new(LineStringType::new(Dimension::XY, metadata));
        for chunk in &chunks {
            for geom in chunk.iter() {
                if let Some(Ok(g)) = geom {
                    bldr.push_line_string(Some(&g.to_line_string().remove_repeated_points()))
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
        let metadata = chunks[0].data_type().metadata().clone();
        let mut bldr =
            MultiLineStringBuilder::new(MultiLineStringType::new(Dimension::XY, metadata));
        for chunk in &chunks {
            for geom in chunk.iter() {
                if let Some(Ok(g)) = geom {
                    bldr.push_multi_line_string(Some(
                        &g.to_multi_line_string().remove_repeated_points(),
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
        let metadata = chunks[0].data_type().metadata().clone();
        let mut bldr = PolygonBuilder::new(PolygonType::new(Dimension::XY, metadata));
        for chunk in &chunks {
            for geom in chunk.iter() {
                if let Some(Ok(g)) = geom {
                    bldr.push_polygon(Some(&g.to_polygon().remove_repeated_points()))
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
        let metadata = chunks[0].data_type().metadata().clone();
        let mut bldr = MultiPolygonBuilder::new(MultiPolygonType::new(Dimension::XY, metadata));
        for chunk in &chunks {
            for geom in chunk.iter() {
                if let Some(Ok(g)) = geom {
                    bldr.push_multi_polygon(Some(&g.to_multi_polygon().remove_repeated_points()))
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
    mod repeated_points;
    fn remove_repeated_points;
}
