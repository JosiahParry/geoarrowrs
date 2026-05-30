use arrow_extendr::IntoArrowRobj;
use extendr_api::prelude::*;
use geo::algorithm::buffer::{Buffer, BufferStyle, LineCap, LineJoin};
use geo_traits::to_geo::ToGeoGeometry;
use geoarrow::{
    array::{GeometryArray, MultiPolygonBuilder},
    datatypes::{Dimension, MultiPolygonType},
};
use geoarrow_array::GeoArrowArrayAccessor;

use crate::{as_geometry_chunks, try_float_array};

#[extendr]
fn buffer(
    geometry: Robj,
    distance: Robj,
    line_cap: &str,
    line_join: &str,
    miter_limit: f64,
    round_segments: f64,
) -> extendr_api::Result<Robj> {
    let dist = try_float_array(distance, "distance")?;

    let parsed_cap = match line_cap {
        "round" => LineCap::Round(round_segments),
        "square" => LineCap::Square,
        "butt" => LineCap::Butt,
        other => {
            return Err(Error::Other(format!(
                "`line_cap` must be one of \"round\", \"square\", or \"butt\", got \"{other}\""
            )));
        }
    };

    let parsed_join = match line_join {
        "round" => LineJoin::Round(round_segments),
        "miter" => LineJoin::Miter(miter_limit),
        "bevel" => LineJoin::Bevel,
        other => {
            return Err(Error::Other(format!(
                "`line_join` must be one of \"round\", \"miter\", or \"bevel\", got \"{other}\""
            )));
        }
    };

    let chunks = as_geometry_chunks(geometry)?;
    let n: usize = chunks.iter().map(|c| c.len()).sum();

    if dist.len() != 1 && dist.len() != n {
        return Err(Error::Other(format!(
            "`distance` must be length 1 or the same length as `geometry` ({}), got {}",
            n,
            dist.len()
        )));
    }

    let metadata = chunks[0].data_type().metadata().clone();
    let mut bldr = MultiPolygonBuilder::new(MultiPolygonType::new(Dimension::XY, metadata));

    for chunk in &chunks {
        let arr = chunk
            .as_any()
            .downcast_ref::<GeometryArray>()
            .ok_or_else(|| Error::Other("expected GeometryArray".to_string()))?;
        for (xi, d) in arr.iter().zip(dist.iter().cycle()) {
            if let (Some(Ok(val)), Some(d)) = (xi, d) {
                let style = BufferStyle::new(d)
                    .line_cap(parsed_cap.clone())
                    .line_join(parsed_join.clone());
                let result = val.to_geometry().buffer_with_style(style);
                bldr.push_multi_polygon(Some(&result))
                    .map_err(|e| Error::Other(e.to_string()))?;
            } else {
                bldr.push_multi_polygon(None::<&geo::MultiPolygon<f64>>)
                    .map_err(|e| Error::Other(e.to_string()))?;
            }
        }
    }

    bldr.finish().into_arrow_robj()
}

extendr_module! {
    mod buffer;
    fn buffer;
}
