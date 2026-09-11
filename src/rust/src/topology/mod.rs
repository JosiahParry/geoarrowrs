use arrow::array::{Array, BooleanBuilder, Int32Builder, StringBuilder};
use arrow_extendr::IntoArrowRobj;
use extendr_api::prelude::*;
use geo::algorithm::coordinate_position::{CoordPos, CoordinatePosition};
use geo::algorithm::dimensions::{Dimensions, HasDimensions};
use geo::algorithm::relate::{IntersectionMatrix, Relate};
use geo::{Coord, Geometry};
use geoarrow_array::GeoArrowArray;

use crate::{as_geo_geometries, as_geometry_chunks, check_recycle_len};

/// Read a geometry argument as one geometry per row, recycled against `n`.
fn as_recycled_geometries(
    robj: Robj,
    n: usize,
    label: &'static str,
) -> extendr_api::Result<Vec<Option<Geometry<f64>>>> {
    let chunks = as_geometry_chunks(robj)?;
    let mut out = Vec::new();
    for chunk in &chunks {
        out.extend(as_geo_geometries(chunk.as_ref())?);
    }
    check_recycle_len(out.len(), n, label)?;
    Ok(out)
}

/// Build the nine character DE-9IM string; Debug wraps it in the type name, so read the cells.
fn de9im(m: &IntersectionMatrix) -> String {
    let order = [CoordPos::Inside, CoordPos::OnBoundary, CoordPos::Outside];
    let mut out = String::with_capacity(9);
    for lhs in order {
        for rhs in order {
            out.push(match m.get(lhs, rhs) {
                Dimensions::Empty => 'F',
                Dimensions::ZeroDimensional => '0',
                Dimensions::OneDimensional => '1',
                Dimensions::TwoDimensional => '2',
            });
        }
    }
    out
}

/// Every binary predicate is the same pairwise walk over one DE-9IM matrix per row.
fn relate_predicate(
    x: Robj,
    y: Robj,
    test: fn(&IntersectionMatrix) -> bool,
) -> extendr_api::Result<Robj> {
    let chunks = as_geometry_chunks(x)?;
    let n: usize = chunks.iter().map(|c| c.len()).sum();
    let others = as_recycled_geometries(y, n, "y")?;
    let mut bldr = BooleanBuilder::with_capacity(n);

    for chunk in &chunks {
        for (geom, other) in as_geo_geometries(chunk.as_ref())?
            .into_iter()
            .zip(others.iter().cycle())
        {
            match (geom, other) {
                (Some(g), Some(o)) => bldr.append_value(test(&g.relate(o))),
                _ => bldr.append_null(),
            }
        }
    }

    bldr.finish().into_data().into_arrow_robj()
}

/// Test a topological relationship
///
/// Each function compares `x` and `y` row by row and returns `TRUE` when the
/// named DE-9IM relationship holds. `y` is recycled against `x`.
///
/// @details
/// `ga_contains()` is `TRUE` when no point of `y` lies outside `x` and at least
/// one point of `y` lies in the interior of `x`. `ga_within()` is the same test
/// with the arguments swapped. `ga_covers()` and `ga_covered_by()` are the weaker
/// forms that allow every shared point to lie on the boundary.
/// `ga_contains_properly()` is the stricter form requiring `y` to fall entirely
/// within the interior.
///
/// `ga_intersects()` and `ga_disjoint()` are negations of one another.
/// `ga_touches()` is `TRUE` when the geometries share a boundary point but no
/// interior point, `ga_crosses()` when their interiors meet in a lower dimension
/// than at least one of them, and `ga_overlaps()` when they meet in the same
/// dimension as both. `ga_equals_topo()` compares point sets rather than
/// coordinate order, so two geometries wound differently are still equal.
///
/// A null geometry on either side gives `NA`.
///
/// @param x a GeoArrow geometry array
/// @param y a GeoArrow geometry array; length 1 or the same length as `x`
/// @returns a boolean array of the same length as `x`
/// @export
/// @rdname topology
/// @family topology
/// @references [Relate](https://docs.rs/geo/latest/geo/algorithm/relate/trait.Relate.html)
/// @examplesIf requireNamespace("sf", quietly = TRUE) && requireNamespace("geoarrow", quietly = TRUE)
/// big <- sf::st_polygon(list(
///   matrix(c(0, 0, 4, 0, 4, 4, 0, 4, 0, 0), ncol = 2, byrow = TRUE)
/// ))
/// x <- geoarrow::as_geoarrow_array(sf::st_sfc(big))
/// y <- geoarrow::as_geoarrow_array(sf::st_sfc(sf::st_point(c(2, 2))))
///
/// as.vector(ga_contains(x, y))
/// as.vector(ga_intersects(x, y))
#[extendr]
fn ga_contains(x: Robj, y: Robj) -> extendr_api::Result<Robj> {
    relate_predicate(x, y, IntersectionMatrix::is_contains)
}

/// @export
/// @rdname topology
/// @family topology
#[extendr]
fn ga_contains_properly(x: Robj, y: Robj) -> extendr_api::Result<Robj> {
    relate_predicate(x, y, IntersectionMatrix::is_contains_properly)
}

/// @export
/// @rdname topology
/// @family topology
#[extendr]
fn ga_within(x: Robj, y: Robj) -> extendr_api::Result<Robj> {
    relate_predicate(x, y, IntersectionMatrix::is_within)
}

/// @export
/// @rdname topology
/// @family topology
#[extendr]
fn ga_covers(x: Robj, y: Robj) -> extendr_api::Result<Robj> {
    relate_predicate(x, y, IntersectionMatrix::is_covers)
}

/// @export
/// @rdname topology
/// @family topology
#[extendr]
fn ga_covered_by(x: Robj, y: Robj) -> extendr_api::Result<Robj> {
    relate_predicate(x, y, IntersectionMatrix::is_coveredby)
}

/// @export
/// @rdname topology
/// @family topology
#[extendr]
fn ga_intersects(x: Robj, y: Robj) -> extendr_api::Result<Robj> {
    relate_predicate(x, y, IntersectionMatrix::is_intersects)
}

/// @export
/// @rdname topology
/// @family topology
#[extendr]
fn ga_disjoint(x: Robj, y: Robj) -> extendr_api::Result<Robj> {
    relate_predicate(x, y, IntersectionMatrix::is_disjoint)
}

/// @export
/// @rdname topology
/// @family topology
#[extendr]
fn ga_touches(x: Robj, y: Robj) -> extendr_api::Result<Robj> {
    relate_predicate(x, y, IntersectionMatrix::is_touches)
}

/// @export
/// @rdname topology
/// @family topology
#[extendr]
fn ga_crosses(x: Robj, y: Robj) -> extendr_api::Result<Robj> {
    relate_predicate(x, y, IntersectionMatrix::is_crosses)
}

/// @export
/// @rdname topology
/// @family topology
#[extendr]
fn ga_overlaps(x: Robj, y: Robj) -> extendr_api::Result<Robj> {
    relate_predicate(x, y, IntersectionMatrix::is_overlaps)
}

/// @export
/// @rdname topology
/// @family topology
#[extendr]
fn ga_equals_topo(x: Robj, y: Robj) -> extendr_api::Result<Robj> {
    relate_predicate(x, y, IntersectionMatrix::is_equal_topo)
}

/// DE-9IM relationship between geometries
///
/// Returns the nine character DE-9IM matrix describing how each pair of
/// geometries relates, from which every named predicate can be derived.
///
/// @details
/// The string reads as the intersections of the interior, boundary and
/// exterior of `x` with those of `y`, in that order. Each character is the
/// dimension of that intersection: `F` for empty, `0` for a point, `1` for a
/// curve, and `2` for a surface. A null geometry on either side gives `NA`.
///
/// @inheritParams ga_contains
/// @returns a string array of the same length as `x`
/// @export
/// @family topology
/// @references [Relate](https://docs.rs/geo/latest/geo/algorithm/relate/trait.Relate.html)
/// @examplesIf requireNamespace("sf", quietly = TRUE) && requireNamespace("geoarrow", quietly = TRUE)
/// big <- sf::st_polygon(list(
///   matrix(c(0, 0, 4, 0, 4, 4, 0, 4, 0, 0), ncol = 2, byrow = TRUE)
/// ))
/// x <- geoarrow::as_geoarrow_array(sf::st_sfc(big))
/// y <- geoarrow::as_geoarrow_array(sf::st_sfc(sf::st_point(c(2, 2))))
///
/// as.vector(ga_relate(x, y))
#[extendr]
fn ga_relate(x: Robj, y: Robj) -> extendr_api::Result<Robj> {
    let chunks = as_geometry_chunks(x)?;
    let n: usize = chunks.iter().map(|c| c.len()).sum();
    let others = as_recycled_geometries(y, n, "y")?;
    let mut bldr = StringBuilder::new();

    for chunk in &chunks {
        for (geom, other) in as_geo_geometries(chunk.as_ref())?
            .into_iter()
            .zip(others.iter().cycle())
        {
            match (geom, other) {
                (Some(g), Some(o)) => bldr.append_value(de9im(&g.relate(o))),
                _ => bldr.append_null(),
            }
        }
    }

    bldr.finish().into_data().into_arrow_robj()
}

/// Determine the topological dimension of geometries
///
/// Returns 0 for points, 1 for lines and curves, and 2 for surfaces.
///
/// @details
/// An empty geometry has no dimension and gives `NA`, which is distinct from a
/// point's 0. `ga_boundary_dimension()` gives the dimension of the geometry's
/// boundary instead, so a polygon is 1 and a point is `NA`.
///
/// @param geometry a GeoArrow geometry array
/// @returns an integer array of the same length as `geometry`
/// @export
/// @rdname ga_dimension
/// @family topology
/// @references [HasDimensions](https://docs.rs/geo/latest/geo/algorithm/dimensions/trait.HasDimensions.html)
/// @examplesIf requireNamespace("sf", quietly = TRUE) && requireNamespace("geoarrow", quietly = TRUE)
/// g <- geoarrow::as_geoarrow_array(sf::st_sfc(
///   sf::st_point(c(0, 0)),
///   sf::st_linestring(cbind(c(0, 1), c(0, 1)))
/// ))
///
/// as.vector(ga_dimension(g))
#[extendr]
fn ga_dimension(geometry: Robj) -> extendr_api::Result<Robj> {
    dimension_impl(geometry, false)
}

/// @export
/// @rdname ga_dimension
/// @family topology
#[extendr]
fn ga_boundary_dimension(geometry: Robj) -> extendr_api::Result<Robj> {
    dimension_impl(geometry, true)
}

/// Shared body for the two dimension functions, which differ only in which one they ask for.
fn dimension_impl(geometry: Robj, boundary: bool) -> extendr_api::Result<Robj> {
    let chunks = as_geometry_chunks(geometry)?;
    let n = chunks.iter().map(|c| c.len()).sum();
    let mut bldr = Int32Builder::with_capacity(n);

    for chunk in &chunks {
        for geom in as_geo_geometries(chunk.as_ref())? {
            let dim = geom.map(|g| {
                if boundary {
                    g.boundary_dimensions()
                } else {
                    g.dimensions()
                }
            });
            match dim {
                Some(Dimensions::ZeroDimensional) => bldr.append_value(0),
                Some(Dimensions::OneDimensional) => bldr.append_value(1),
                Some(Dimensions::TwoDimensional) => bldr.append_value(2),
                Some(Dimensions::Empty) | None => bldr.append_null(),
            }
        }
    }

    bldr.finish().into_data().into_arrow_robj()
}

/// Test whether geometries are empty
///
/// `TRUE` when the geometry holds no coordinates.
///
/// @details
/// An empty geometry is distinct from a null one. A null geometry gives `NA`.
///
/// @param geometry a GeoArrow geometry array
/// @returns a boolean array of the same length as `geometry`
/// @export
/// @family topology
/// @references [HasDimensions](https://docs.rs/geo/latest/geo/algorithm/dimensions/trait.HasDimensions.html)
/// @examplesIf requireNamespace("sf", quietly = TRUE) && requireNamespace("geoarrow", quietly = TRUE)
/// g <- geoarrow::as_geoarrow_array(sf::st_sfc(
///   sf::st_point(c(0, 0)),
///   sf::st_point()
/// ))
///
/// as.vector(ga_is_empty(g))
#[extendr]
fn ga_is_empty(geometry: Robj) -> extendr_api::Result<Robj> {
    let chunks = as_geometry_chunks(geometry)?;
    let n = chunks.iter().map(|c| c.len()).sum();
    let mut bldr = BooleanBuilder::with_capacity(n);

    for chunk in &chunks {
        for geom in as_geo_geometries(chunk.as_ref())? {
            match geom {
                Some(g) => bldr.append_value(g.is_empty()),
                None => bldr.append_null(),
            }
        }
    }

    bldr.finish().into_data().into_arrow_robj()
}

/// Locate a point relative to a geometry
///
/// Returns `"inside"`, `"outside"`, or `"boundary"` for each pair.
///
/// @details
/// This is the three way form of a point in polygon test, distinguishing a
/// point that lies exactly on an edge from one strictly inside. `point` is
/// recycled against `geometry`. A null geometry or a null point gives `NA`.
///
/// @param geometry a GeoArrow geometry array
/// @param point a GeoArrow point array; length 1 or the same length as `geometry`
/// @returns a string array of the same length as `geometry`
/// @export
/// @family topology
/// @references [CoordinatePosition](https://docs.rs/geo/latest/geo/algorithm/coordinate_position/trait.CoordinatePosition.html)
/// @examplesIf requireNamespace("sf", quietly = TRUE) && requireNamespace("geoarrow", quietly = TRUE)
/// ring <- rbind(c(0, 0), c(4, 0), c(4, 4), c(0, 4), c(0, 0))
/// square <- sf::st_polygon(list(ring))
/// g <- geoarrow::as_geoarrow_array(sf::st_sfc(square, square, square))
///
/// # inside, on the edge, and outside
/// p <- ga_xy(c(2, 0, 9), c(2, 2, 9))
/// as.vector(ga_coordinate_position(g, p))
#[extendr]
fn ga_coordinate_position(geometry: Robj, point: Robj) -> extendr_api::Result<Robj> {
    let chunks = as_geometry_chunks(geometry)?;
    let n: usize = chunks.iter().map(|c| c.len()).sum();
    let points = as_recycled_geometries(point, n, "point")?;
    let mut bldr = StringBuilder::new();

    for chunk in &chunks {
        for (geom, p) in as_geo_geometries(chunk.as_ref())?
            .into_iter()
            .zip(points.iter().cycle())
        {
            let coord: Option<Coord<f64>> = match p {
                Some(Geometry::Point(p)) => Some(p.0),
                _ => None,
            };
            match (geom, coord) {
                (Some(g), Some(c)) => match g.coordinate_position(&c) {
                    CoordPos::Inside => bldr.append_value("inside"),
                    CoordPos::Outside => bldr.append_value("outside"),
                    CoordPos::OnBoundary => bldr.append_value("boundary"),
                },
                _ => bldr.append_null(),
            }
        }
    }

    bldr.finish().into_data().into_arrow_robj()
}

extendr_module! {
    mod topology;
    fn ga_contains;
    fn ga_contains_properly;
    fn ga_within;
    fn ga_covers;
    fn ga_covered_by;
    fn ga_intersects;
    fn ga_disjoint;
    fn ga_touches;
    fn ga_crosses;
    fn ga_overlaps;
    fn ga_equals_topo;
    fn ga_relate;
    fn ga_dimension;
    fn ga_boundary_dimension;
    fn ga_is_empty;
    fn ga_coordinate_position;
}
