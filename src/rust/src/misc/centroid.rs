use arrow_extendr::IntoArrowRobj;
use extendr_api::prelude::*;
use geo::Centroid;
use geo_traits::to_geo::ToGeoGeometry;
use geoarrow::{
    array::{GeometryArray, PointBuilder},
    datatypes::{Dimension, PointType},
};
use geoarrow_array::GeoArrowArrayAccessor;

use crate::as_geometry_chunks;

#[extendr]
fn centroid(x: Robj) -> extendr_api::Result<Robj> {
    let chunks = as_geometry_chunks(x)?;
    let n = chunks.iter().map(|c| c.len()).sum();
    let metadata = chunks[0].data_type().metadata().clone();
    let mut bldr = PointBuilder::new(PointType::new(Dimension::XY, metadata));
    bldr.reserve(n);

    for chunk in &chunks {
        let arr = chunk
            .as_any()
            .downcast_ref::<GeometryArray>()
            .ok_or_else(|| Error::Other("expected GeometryArray".to_string()))?;
        for xi in arr.iter() {
            if let Some(Ok(val)) = xi {
                match val.to_geometry().centroid() {
                    Some(pt) => bldr.push_point(Some(&pt)),
                    None => bldr.push_point(None::<&geo::Point<f64>>),
                }
            } else {
                bldr.push_point(None::<&geo::Point<f64>>);
            }
        }
    }

    bldr.finish().into_arrow_robj()
}

extendr_module! {
    mod centroid;
    fn centroid;
}
