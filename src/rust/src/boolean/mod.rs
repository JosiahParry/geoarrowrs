use arrow_extendr::IntoArrowRobj;
use extendr_api::prelude::*;
use geo::algorithm::bool_ops::{BooleanOps, unary_union as geo_unary_union};
use geo::{Geometry, MultiPolygon, Polygon};
use geoarrow::array::MultiPolygonBuilder;
use geoarrow::datatypes::{Dimension, MultiPolygonType};
use geoarrow_array::GeoArrowArray;

use crate::{as_geo_geometries, as_geometry_chunks, check_recycle_len};

/// Which pairwise set operation to apply.
#[derive(Clone, Copy)]
enum Op {
    Intersection,
    Union,
    Difference,
    Xor,
}

/// BooleanOps covers polygons and multipolygons only, so anything else is a null row.
fn as_polygonal(g: &Geometry<f64>) -> Option<MultiPolygon<f64>> {
    match g {
        Geometry::Polygon(p) => Some(MultiPolygon(vec![p.clone()])),
        Geometry::MultiPolygon(mp) => Some(mp.clone()),
        _ => None,
    }
}

/// Read a geometry argument as one polygonal geometry per row, recycled against `n`.
fn as_recycled_polygonal(
    robj: Robj,
    n: usize,
    label: &'static str,
) -> extendr_api::Result<Vec<Option<MultiPolygon<f64>>>> {
    let chunks = as_geometry_chunks(robj)?;
    let mut out = Vec::new();
    for chunk in &chunks {
        for geom in as_geo_geometries(chunk.as_ref())? {
            out.push(geom.as_ref().and_then(as_polygonal));
        }
    }
    check_recycle_len(out.len(), n, label)?;
    Ok(out)
}

/// The four set operations share one pairwise walk and differ only in the op applied.
fn boolean_pairwise(x: Robj, y: Robj, op: Op) -> extendr_api::Result<Robj> {
    let chunks = as_geometry_chunks(x)?;
    let n: usize = chunks.iter().map(|c| c.len()).sum();
    let others = as_recycled_polygonal(y, n, "y")?;

    let metadata = chunks[0].data_type().metadata().clone();
    let mut bldr = MultiPolygonBuilder::new(MultiPolygonType::new(Dimension::XY, metadata));

    for chunk in &chunks {
        for (geom, other) in as_geo_geometries(chunk.as_ref())?
            .into_iter()
            .zip(others.iter().cycle())
        {
            let lhs = geom.as_ref().and_then(as_polygonal);
            let result = match (lhs, other) {
                (Some(a), Some(b)) => Some(match op {
                    Op::Intersection => a.intersection(b),
                    Op::Union => a.union(b),
                    Op::Difference => a.difference(b),
                    Op::Xor => a.xor(b),
                }),
                _ => None,
            };
            bldr.push_multi_polygon(result.as_ref())
                .map_err(|e| Error::Other(e.to_string()))?;
        }
    }

    bldr.finish().into_arrow_robj()
}

/// Combine two polygon arrays with a set operation
///
/// Each function pairs `x` with `y` row by row and returns the polygonal
/// result. `y` is recycled against `x`.
///
/// @details
/// `ga_boolean_intersection()` keeps the area in both, `ga_boolean_union()` the area
/// in either, `ga_boolean_difference()` the area in `x` but not `y`, and
/// `ga_boolean_xor()` the area in exactly one of them.
///
/// These are defined for polygons and multipolygons only. A row whose geometry
/// is any other type, or is null, comes back null. The result is always a
/// multipolygon, since a set operation can split one polygon into several or
/// erase it entirely.
///
/// @param x a GeoArrow polygon or multipolygon array
/// @param y a GeoArrow polygon or multipolygon array; length 1 or the same
///   length as `x`
/// @returns a GeoArrow multipolygon array of the same length as `x`
/// @export
/// @rdname boolean_ops
/// @family boolean
/// @references [BooleanOps](https://docs.rs/geo/latest/geo/algorithm/bool_ops/trait.BooleanOps.html)
/// @examplesIf requireNamespace("sf", quietly = TRUE) && requireNamespace("geoarrow", quietly = TRUE)
/// x <- geoarrow::as_geoarrow_array(sf::st_sfc(
///   sf::st_polygon(list(rbind(c(0, 0), c(2, 0), c(2, 2), c(0, 2), c(0, 0))))
/// ))
/// y <- geoarrow::as_geoarrow_array(sf::st_sfc(
///   sf::st_polygon(list(rbind(c(1, 1), c(3, 1), c(3, 3), c(1, 3), c(1, 1))))
/// ))
///
/// # two 2x2 squares offset by 1, so the overlap is a 1x1 square
/// as.vector(ga_unsigned_area(ga_boolean_intersection(x, y)))
#[extendr]
fn ga_boolean_intersection(x: Robj, y: Robj) -> extendr_api::Result<Robj> {
    boolean_pairwise(x, y, Op::Intersection)
}

/// @export
/// @rdname boolean_ops
/// @family boolean
#[extendr]
fn ga_boolean_union(x: Robj, y: Robj) -> extendr_api::Result<Robj> {
    boolean_pairwise(x, y, Op::Union)
}

/// @export
/// @rdname boolean_ops
/// @family boolean
#[extendr]
fn ga_boolean_difference(x: Robj, y: Robj) -> extendr_api::Result<Robj> {
    boolean_pairwise(x, y, Op::Difference)
}

/// @export
/// @rdname boolean_ops
/// @family boolean
#[extendr]
fn ga_boolean_xor(x: Robj, y: Robj) -> extendr_api::Result<Robj> {
    boolean_pairwise(x, y, Op::Xor)
}

/// Dissolve an entire array of polygons into one
///
/// Merges every polygon in `x` into a single multipolygon, dropping the
/// boundaries between any that touch or overlap.
///
/// @details
/// This is the one function in the package that is not length-preserving. It
/// is an aggregate over the whole array, so it always returns a length 1
/// array, in the way `sum()` reduces a vector to a single value.
///
/// It is far faster than folding [ga_boolean_union()] across the array, since it
/// unions all the rings in one pass. Rows that are not polygonal, and null
/// rows, are skipped.
///
/// @param x a GeoArrow polygon or multipolygon array
/// @returns a GeoArrow multipolygon array of length 1
/// @export
/// @family boolean
/// @references [unary_union](https://docs.rs/geo/latest/geo/algorithm/bool_ops/fn.unary_union.html)
/// @examplesIf requireNamespace("sf", quietly = TRUE) && requireNamespace("geoarrow", quietly = TRUE)
/// x <- geoarrow::as_geoarrow_array(sf::st_sfc(
///   sf::st_polygon(list(rbind(c(0, 0), c(2, 0), c(2, 2), c(0, 2), c(0, 0)))),
///   sf::st_polygon(list(rbind(c(1, 1), c(3, 1), c(3, 3), c(1, 3), c(1, 1))))
/// ))
///
/// # 4 + 4 less the 1 they share
/// as.vector(ga_unsigned_area(ga_unary_union(x)))
#[extendr]
fn ga_unary_union(x: Robj) -> extendr_api::Result<Robj> {
    let chunks = as_geometry_chunks(x)?;
    let metadata = chunks[0].data_type().metadata().clone();

    let mut parts: Vec<Polygon<f64>> = Vec::new();
    for chunk in &chunks {
        for geom in as_geo_geometries(chunk.as_ref())? {
            if let Some(mp) = geom.as_ref().and_then(as_polygonal) {
                parts.extend(mp.0);
            }
        }
    }

    let dissolved = geo_unary_union(parts.iter());
    let mut bldr = MultiPolygonBuilder::new(MultiPolygonType::new(Dimension::XY, metadata));
    bldr.push_multi_polygon(Some(&dissolved))
        .map_err(|e| Error::Other(e.to_string()))?;

    bldr.finish().into_arrow_robj()
}

extendr_module! {
    mod boolean;
    fn ga_boolean_intersection;
    fn ga_boolean_union;
    fn ga_boolean_difference;
    fn ga_boolean_xor;
    fn ga_unary_union;
}
