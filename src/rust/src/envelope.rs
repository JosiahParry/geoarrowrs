use arrow_extendr::IntoArrowRobj;
use extendr_api::prelude::*;
use geo::BoundingRect;
use geoarrow::array::RectBuilder;
use geoarrow::datatypes::{BoxType, Dimension, GeoArrowType};
use geoarrow_array::GeoArrowArray;
use std::sync::Arc;

use crate::as_geometry_chunks;

/// The box of one geometry, taken as it is when the geometry is already a box.
pub(crate) fn rect_of(geom: Option<&geo::Geometry<f64>>) -> Option<geo::Rect<f64>> {
    match geom {
        Some(geo::Geometry::Rect(r)) => Some(*r),
        Some(g) => g.bounding_rect(),
        None => None,
    }
}

/// Read any geoarrow array as the axis aligned boxes an index query needs.
///
/// A box array is returned untouched, so nothing recomputes what the caller
/// already has.
pub(crate) fn as_rects(
    chunks: &[Arc<dyn GeoArrowArray>],
) -> extendr_api::Result<Vec<Option<geo::Rect<f64>>>> {
    Ok(rects_of(&crate::as_geo_geometries_par(chunks)?))
}

/// The box of every geometry, one rayon task per slice.
pub(crate) fn rects_of(geoms: &[Option<geo::Geometry<f64>>]) -> Vec<Option<geo::Rect<f64>>> {
    use rayon::prelude::*;
    crate::threads::with_pool(|| geoms.par_iter().map(|g| rect_of(g.as_ref())).collect())
}

/// Compute the bounding box of geometries
///
/// Returns the axis aligned bounding box of each geometry, as the box array
/// that the spatial index queries take.
///
/// @details
/// A box array is passed straight through rather than recomputed, so this is
/// cheap to call on something that already holds boxes and safe to call
/// defensively before a query.
///
/// This is the envelope in the OGC sense. It differs from
/// [ga_bounding_rect()] only in that pass through; the boxes themselves are
/// the same.
///
/// @param x a GeoArrow geometry array
/// @returns a GeoArrow box array of the same length as `x`
/// @export
/// @family index
/// @examplesIf requireNamespace("sf", quietly = TRUE) && requireNamespace("geoarrow", quietly = TRUE)
/// nc <- as.data.frame(read_shapefile(
///   system.file("shape/nc.shp", package = "sf")
/// ))
/// boxes <- ga_envelope(nc$geometry)
/// head(geoarrow::as_geoarrow_vctr(boxes), 3)
///
/// # already boxes, so this is a no op
/// identical(
///   as.character(geoarrow::as_geoarrow_vctr(ga_envelope(boxes))),
///   as.character(geoarrow::as_geoarrow_vctr(boxes))
/// )
#[extendr]
fn ga_envelope(x: Robj) -> anyhow::Result<Robj> {
    let chunks = as_geometry_chunks(x).map_err(|e| anyhow::anyhow!("{e}"))?;

    if matches!(chunks[0].data_type(), GeoArrowType::Rect(_)) && chunks.len() == 1 {
        return chunks[0]
            .clone()
            .into_arrow_robj()
            .map_err(|e| anyhow::anyhow!("{e}"));
    }

    let metadata = chunks[0].data_type().metadata().clone();
    let mut bldr = RectBuilder::new(BoxType::new(Dimension::XY, metadata));
    for rect in as_rects(&chunks).map_err(|e| anyhow::anyhow!("{e}"))? {
        bldr.push_rect(rect.as_ref());
    }

    bldr.finish()
        .into_arrow_robj()
        .map_err(|e| anyhow::anyhow!("{e}"))
}

extendr_module! {
    mod envelope;
    fn ga_envelope;
}
