use arrow_extendr::IntoArrowRobj;
use extendr_api::prelude::*;
use geo::{BoundingRect, MinimumRotatedRect, Polygon};
use geo_traits::to_geo::ToGeoGeometry;
use geoarrow::{
    array::{GeometryArray, PolygonBuilder, RectBuilder},
    datatypes::{BoxType, Dimension, PolygonType},
};
use geoarrow_array::GeoArrowArrayAccessor;

use crate::as_geometry_chunks;

#[extendr]
fn bounding_rect(x: Robj) -> extendr_api::Result<Robj> {
    let chunks = as_geometry_chunks(x)?;
    let n = chunks.iter().map(|c| c.len()).sum();
    let metadata = chunks[0].data_type().metadata().clone();
    let mut bldr = RectBuilder::new(BoxType::new(geoarrow::datatypes::Dimension::XY, metadata));
    bldr.reserve(n);

    for chunk in &chunks {
        let arr = chunk
            .as_any()
            .downcast_ref::<GeometryArray>()
            .ok_or_else(|| Error::Other("expected GeometryArray".to_string()))?;
        for xi in arr.iter() {
            if let Some(Ok(val)) = xi {
                bldr.push_rect(val.to_geometry().bounding_rect().as_ref());
            } else {
                bldr.push_rect(None::<&geo::Rect<f64>>);
            }
        }
    }

    bldr.finish().into_arrow_robj()
}

#[extendr]
fn minimum_rotated_rect(x: Robj) -> extendr_api::Result<Robj> {
    let chunks = as_geometry_chunks(x)?;
    let n = chunks.iter().map(|c| c.len()).sum();
    let metadata = chunks[0].data_type().metadata().clone();
    let mut bldr = PolygonBuilder::new(PolygonType::new(Dimension::XY, metadata));
    bldr.reserve(n);

    for chunk in &chunks {
        let arr = chunk
            .as_any()
            .downcast_ref::<GeometryArray>()
            .ok_or_else(|| Error::Other("expected GeometryArray".to_string()))?;
        for xi in arr.iter() {
            if let Some(Ok(val)) = xi {
                bldr.push_polygon(val.to_geometry().minimum_rotated_rect().as_ref())
                    .map_err(|e| Error::Other(e.to_string()))?;
            } else {
                bldr.push_polygon(None::<&Polygon<f64>>)
                    .map_err(|e| Error::Other(e.to_string()))?;
            }
        }
    }

    bldr.finish().into_arrow_robj()
}

extendr_module! {
    mod boundary;
    fn bounding_rect;
    fn minimum_rotated_rect;
}
