use arrow_extendr::IntoArrowRobj;
use extendr_api::prelude::*;
use geo::algorithm::buffer::{Buffer, BufferStyle, LineCap, LineJoin};
use geoarrow::{
    array::MultiPolygonBuilder,
    datatypes::{Dimension, MultiPolygonType},
};
use geoarrow_array::GeoArrowArray;

use crate::{as_geo_geometries, as_geometry_chunks, try_float_array};

/// Buffer geometries by a given distance
///
/// Expands each geometry outward by `distance` to produce a multipolygon.
/// Line cap and join styles control the shape of the buffer at endpoints and
/// corners.
///
/// @param geometry a GeoArrow geometry array
/// @param distance a numeric vector of buffer distances; length 1 or the same length as `geometry`
/// @param line_cap one of `"round"`, `"square"`, or `"butt"`
/// @param line_join one of `"round"`, `"miter"`, or `"bevel"`
/// @param miter_limit the miter limit used when `line_join` is `"miter"`
/// @param round_angle the angular step in radians used to approximate curves when `line_cap` or `line_join` is `"round"`. Smaller is smoother
/// @returns a GeoArrow multipolygon array
/// @export
/// @family misc
/// @references [Buffer](https://docs.rs/geo/latest/geo/algorithm/buffer/trait.Buffer.html)
/// @examplesIf requireNamespace("sf", quietly = TRUE) && requireNamespace("geoarrow", quietly = TRUE)
/// sq <- geoarrow::as_geoarrow_array(sf::st_sfc(
///   sf::st_polygon(list(rbind(c(0, 0), c(2, 0), c(2, 2), c(0, 2), c(0, 0))))
/// ))
///
/// # the 2x2 square grows by 8 along its sides plus a unit circle at the corners
/// as.vector(ga_unsigned_area(ga_buffer(sq, 1)))
///
/// # a smaller angular step rounds the corners more finely
/// as.vector(ga_unsigned_area(ga_buffer(sq, 1, round_angle = 0.01)))
///
/// # square corners instead
/// as.vector(ga_unsigned_area(ga_buffer(sq, 1, line_join = "bevel")))
#[extendr]
fn ga_buffer(
    geometry: Robj,
    distance: Robj,
    #[extendr(default = "\"round\"")] line_cap: &str,
    #[extendr(default = "\"round\"")] line_join: &str,
    #[extendr(default = "2.0")] miter_limit: f64,
    #[extendr(default = "0.2")] round_angle: f64,
) -> extendr_api::Result<Robj> {
    let dist = try_float_array(distance, "distance")?;

    let parsed_cap = match line_cap {
        "round" => LineCap::Round(round_angle),
        "square" => LineCap::Square,
        "butt" => LineCap::Butt,
        other => {
            return Err(Error::Other(format!(
                "`line_cap` must be one of \"round\", \"square\", or \"butt\", got \"{other}\""
            )));
        }
    };

    let parsed_join = match line_join {
        "round" => LineJoin::Round(round_angle),
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
        for (geom, d) in as_geo_geometries(chunk.as_ref())?
            .into_iter()
            .zip(dist.iter().cycle())
        {
            if let (Some(val), Some(d)) = (geom, d) {
                let style = BufferStyle::new(d)
                    .line_cap(parsed_cap.clone())
                    .line_join(parsed_join.clone());
                let result = val.buffer_with_style(style);
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
    fn ga_buffer;
}
