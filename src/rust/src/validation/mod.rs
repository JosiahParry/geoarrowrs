use arrow::array::{Array, BooleanBuilder, StringBuilder};
use arrow_extendr::IntoArrowRobj;
use extendr_api::prelude::*;
use geo::algorithm::validation::Validation;
use geo::{Geometry, MakeValid};
use geoarrow::datatypes::{Dimension, GeoArrowType, MultiPolygonType};
use geoarrow_array::GeoArrowArray;
use rayon::prelude::*;

use crate::cast::CastGeoArrow;
use crate::threads::{Threads, with_pool};
use crate::{as_geo_geometries, as_geo_geometries_par, as_geometry_chunks};

/// Test whether geometries are well formed
///
/// `TRUE` when a geometry satisfies the OGC simple features rules.
///
/// @details
/// Several algorithms give wrong answers on invalid input rather than failing,
/// so check validity before trusting a result. Common problems are a polygon
/// whose rings self intersect, a ring with too few points, and a coordinate
/// that is `NaN` or infinite.
///
/// `ga_is_valid()` returns `NA` for a null geometry. Use [ga_validation_error()]
/// to see why a geometry is invalid.
///
/// @param geometry a GeoArrow geometry array
/// @returns a boolean array of the same length as `geometry`
/// @export
/// @rdname validation
/// @family validation
/// @references [Validation](https://docs.rs/geo/latest/geo/algorithm/validation/trait.Validation.html)
/// @examplesIf requireNamespace("sf", quietly = TRUE) && requireNamespace("geoarrow", quietly = TRUE)
/// square <- rbind(c(0, 0), c(2, 0), c(2, 2), c(0, 2), c(0, 0))
/// bowtie <- rbind(c(0, 0), c(2, 2), c(2, 0), c(0, 2), c(0, 0))
/// good <- sf::st_polygon(list(square))
/// bad <- sf::st_polygon(list(bowtie))
/// g <- geoarrow::as_geoarrow_array(sf::st_sfc(good, bad))
///
/// as.vector(ga_is_valid(g))
/// as.vector(ga_validation_error(g))
#[extendr]
fn ga_is_valid(geometry: Robj) -> anyhow::Result<Robj> {
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
/// Returns the first validation problem it finds, or `NA` when the geometry is
/// valid.
///
/// @details
/// A geometry can break more than one rule; `ga_validation_error()` reports only
/// the first, since you often resolve the rest by fixing it.
///
/// @returns a string array of the same length as `geometry`
/// @export
/// @rdname validation
/// @family validation
#[extendr]
fn ga_validation_error(geometry: Robj) -> anyhow::Result<Robj> {
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

/// Repair invalid polygons
///
/// Returns a valid multipolygon for each polygon, multipolygon, or box,
/// splitting a self intersecting ring at its crossings and removing dangling
/// edges and holes that cancel out. Use [ga_is_valid()] to find the rows that
/// need it.
///
/// @details
/// `ga_make_valid()` uses the prepair algorithm of Ledoux, Arroyo Ohori, and
/// Meijers (2014): it triangulates the rings and keeps each triangle that an
/// odd number of ring edges separates from the outside. It always returns a
/// multipolygon, since one invalid polygon can split into several valid
/// parts.
///
/// The odd-even rule gives the same shape as `sf::st_make_valid()` for a
/// self intersecting ring, a dangling spike, and a hole that touches or leaves
/// its shell, but not where rings overlap. sf's default GEOS method subtracts
/// the union of the holes and unions overlapping parts, while
/// `ga_make_valid()` keeps the overlap of two holes and a hole inside a hole as
/// islands, and drops the overlap of two parts.
///
/// `ga_make_valid()` returns an empty multipolygon for an empty polygon. It
/// returns a null for a null geometry, for a geometry that is not polygonal,
/// and for a polygon it cannot repair, such as one with a `NaN` or infinite
/// coordinate.
///
/// @param geometry a GeoArrow geometry array
/// @returns a GeoArrow multipolygon array of the same length as `geometry`
/// @export
/// @family validation
/// @references [MakeValid](https://docs.rs/geo/latest/geo/algorithm/repair_polygon/trait.MakeValid.html)
/// @examplesIf requireNamespace("sf", quietly = TRUE) && requireNamespace("geoarrow", quietly = TRUE)
/// bowtie <- rbind(c(0, 0), c(2, 2), c(2, 0), c(0, 2), c(0, 0))
/// g <- geoarrow::as_geoarrow_array(sf::st_sfc(sf::st_polygon(list(bowtie))))
/// as.vector(ga_is_valid(g))
///
/// # the crossing splits the bowtie into two triangles
/// fixed <- ga_make_valid(g)
/// sf::st_as_sfc(geoarrow::as_geoarrow_vctr(fixed))
/// as.vector(ga_is_valid(fixed))
#[extendr]
fn ga_make_valid(geometry: Robj) -> extendr_api::Result<Robj> {
    let chunks = as_geometry_chunks(geometry)?;
    let metadata = chunks[0].data_type().metadata().clone();
    let geoms = as_geo_geometries_par(Threads::get(), &chunks)?;

    let repaired = with_pool(Threads::get(), || {
        geoms
            .par_iter()
            .map(|geom| {
                // a geometry that is not polygonal, or that the repair rejects, has no answer
                let fixed = match geom {
                    Some(Geometry::Polygon(p)) => p.make_valid().ok(),
                    Some(Geometry::MultiPolygon(mp)) => mp.make_valid().ok(),
                    Some(Geometry::Rect(r)) => r.to_polygon().make_valid().ok(),
                    _ => None,
                };
                fixed.map(Geometry::MultiPolygon)
            })
            .collect::<Vec<_>>()
    });

    let to_type = GeoArrowType::MultiPolygon(MultiPolygonType::new(Dimension::XY, metadata));
    repaired.cast_geoarrow(&to_type)?.into_arrow_robj()
}

extendr_module! {
    mod validation;
    fn ga_is_valid;
    fn ga_validation_error;
    fn ga_make_valid;
}
