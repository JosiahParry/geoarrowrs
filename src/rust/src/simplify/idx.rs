use arrow::array::{Array, Int32Builder, ListBuilder};
use arrow_extendr::IntoArrowRobj;
use extendr_api::prelude::*;
use geo::Geometry;
use geo::algorithm::simplify::SimplifyIdx;
use geo::algorithm::simplify_vw::SimplifyVwIdx;
use geoarrow_array::GeoArrowArray;

use crate::{as_geo_geometries, as_geometry_chunks, check_recycle_len, try_float_array};

/// Both index variants are defined for linestrings only.
fn retained(g: &Geometry<f64>, epsilon: f64, vw: bool) -> Option<Vec<usize>> {
    match g {
        Geometry::LineString(ls) => Some(if vw {
            ls.simplify_vw_idx(epsilon)
        } else {
            ls.simplify_idx(epsilon)
        }),
        _ => None,
    }
}

/// Find which coordinates simplification would keep
///
/// Returns the positions of the coordinates that survive simplification,
/// rather than the simplified geometry itself. One list per input geometry.
///
/// @details
/// Use these when the coordinates carry data of their own, such as a timestamp
/// or a sensor reading per vertex. Simplifying the geometry discards that
/// alignment; the indices let you subset the other columns the same way.
///
/// `simplify_idx()` uses Ramer-Douglas-Peucker and `simplify_vw_idx()` uses
/// Visvalingam-Whyatt, matching [simplify()] and [simplify_vw()]. Indices are
/// 1 based and always include the first and last coordinate.
///
/// Only linestrings can be simplified this way. Any other geometry type, or a
/// null geometry, comes back null.
///
/// @param geometry a GeoArrow linestring array
/// @param epsilon the simplification tolerance; length 1 or the same length as
///   `geometry`
/// @returns a list array of integer positions, one list per input geometry
/// @export
/// @rdname simplify_idx
/// @family simplify
/// @references [SimplifyIdx](https://docs.rs/geo/latest/geo/algorithm/simplify/trait.SimplifyIdx.html)
/// @examplesIf requireNamespace("sf", quietly = TRUE) && requireNamespace("geoarrow", quietly = TRUE)
/// line <- sf::st_linestring(cbind(c(0, 1, 2, 3, 4), c(0, 0.1, 0, 0.1, 0)))
/// g <- geoarrow::as_geoarrow_array(sf::st_sfc(line))
///
/// nanoarrow::convert_array(simplify_idx(g, 0.5))
#[extendr]
fn simplify_idx(geometry: Robj, epsilon: Robj) -> anyhow::Result<Robj> {
    idx_impl(geometry, epsilon, false)
}

/// @export
/// @rdname simplify_idx
/// @family simplify
/// @references [SimplifyVwIdx](https://docs.rs/geo/latest/geo/algorithm/simplify_vw/trait.SimplifyVwIdx.html)
#[extendr]
fn simplify_vw_idx(geometry: Robj, epsilon: Robj) -> anyhow::Result<Robj> {
    idx_impl(geometry, epsilon, true)
}

/// Shared body for the two index variants, which differ only in the algorithm.
fn idx_impl(geometry: Robj, epsilon: Robj, vw: bool) -> anyhow::Result<Robj> {
    let epsilons = try_float_array(epsilon, "epsilon").map_err(|e| anyhow::anyhow!("{e}"))?;
    let chunks = as_geometry_chunks(geometry).map_err(|e| anyhow::anyhow!("{e}"))?;
    let n: usize = chunks.iter().map(|c| c.len()).sum();
    check_recycle_len(epsilons.len(), n, "epsilon").map_err(|e| anyhow::anyhow!("{e}"))?;

    let mut bldr = ListBuilder::new(Int32Builder::new());

    for chunk in &chunks {
        let geoms = as_geo_geometries(chunk.as_ref()).map_err(|e| anyhow::anyhow!("{e}"))?;
        for (geom, eps) in geoms.into_iter().zip(epsilons.iter().cycle()) {
            let keep = match (geom, eps) {
                (Some(g), Some(eps)) => retained(&g, eps, vw),
                _ => None,
            };
            match keep {
                Some(idx) => {
                    for i in idx {
                        bldr.values().append_value(i as i32 + 1);
                    }
                    bldr.append(true);
                }
                None => bldr.append(false),
            }
        }
    }

    bldr.finish()
        .into_data()
        .into_arrow_robj()
        .map_err(|e| anyhow::anyhow!("{e}"))
}

extendr_module! {
    mod idx;
    fn simplify_idx;
    fn simplify_vw_idx;
}
