use arrow::array::{Array, BooleanBuilder, StringBuilder};
use arrow_extendr::IntoArrowRobj;
use extendr_api::prelude::*;
use geo::algorithm::validation::Validation;
use geoarrow_array::GeoArrowArray;

use crate::{as_geo_geometries, as_geometry_chunks};

/// Test whether geometries are well formed
///
/// `TRUE` when a geometry satisfies the OGC simple features rules.
///
/// @details
/// Several algorithms give wrong answers on invalid input rather than failing,
/// so this is worth checking before trusting a result. Common problems are a
/// polygon whose rings self intersect, a ring with too few points, and a
/// coordinate that is `NaN` or infinite.
///
/// A null geometry gives `NA`. Use [validation_error()] to see why a geometry
/// failed.
///
/// @param geometry a GeoArrow geometry array
/// @returns a boolean array of the same length as `geometry`
/// @export
/// @rdname validation
/// @family validation
/// @references [Validation](https://docs.rs/geo/latest/geo/algorithm/validation/trait.Validation.html)
/// @examplesIf requireNamespace("sf", quietly = TRUE) && requireNamespace("geoarrow", quietly = TRUE)
/// good <- sf::st_polygon(list(matrix(
///   c(0, 0, 2, 0, 2, 2, 0, 2, 0, 0), ncol = 2, byrow = TRUE
/// )))
/// bowtie <- sf::st_polygon(list(matrix(
///   c(0, 0, 2, 2, 2, 0, 0, 2, 0, 0), ncol = 2, byrow = TRUE
/// )))
/// g <- geoarrow::as_geoarrow_array(sf::st_sfc(good, bowtie))
///
/// as.vector(nanoarrow::convert_array(is_valid(g)))
/// as.vector(nanoarrow::convert_array(validation_error(g)))
#[extendr]
fn is_valid(geometry: Robj) -> anyhow::Result<Robj> {
    let chunks = as_geometry_chunks(geometry).map_err(|e| anyhow::anyhow!("{e}"))?;
    let n = chunks.iter().map(|c| c.len()).sum();
    let mut bldr = BooleanBuilder::with_capacity(n);

    for chunk in &chunks {
        let geoms = as_geo_geometries(chunk.as_ref()).map_err(|e| anyhow::anyhow!("{e}"))?;
        for geom in geoms {
            match geom {
                Some(g) => bldr.append_value(g.is_valid()),
                None => bldr.append_null(),
            }
        }
    }

    bldr.finish()
        .into_data()
        .into_arrow_robj()
        .map_err(|e| anyhow::anyhow!("{e}"))
}

/// Explain why a geometry is invalid
///
/// Returns the first validation problem found, or `NA` when the geometry is
/// valid.
///
/// @details
/// A geometry can break more than one rule; only the first is reported, since
/// fixing it often resolves the rest.
///
/// @returns a string array of the same length as `geometry`
/// @export
/// @rdname validation
/// @family validation
/// @references [Validation](https://docs.rs/geo/latest/geo/algorithm/validation/trait.Validation.html)
#[extendr]
fn validation_error(geometry: Robj) -> anyhow::Result<Robj> {
    let chunks = as_geometry_chunks(geometry).map_err(|e| anyhow::anyhow!("{e}"))?;
    let mut bldr = StringBuilder::new();

    for chunk in &chunks {
        let geoms = as_geo_geometries(chunk.as_ref()).map_err(|e| anyhow::anyhow!("{e}"))?;
        for geom in geoms {
            match geom.map(|g| g.validation_errors()) {
                Some(errors) if !errors.is_empty() => {
                    bldr.append_value(errors[0].to_string());
                }
                _ => bldr.append_null(),
            }
        }
    }

    bldr.finish()
        .into_data()
        .into_arrow_robj()
        .map_err(|e| anyhow::anyhow!("{e}"))
}

extendr_module! {
    mod validation;
    fn is_valid;
    fn validation_error;
}
