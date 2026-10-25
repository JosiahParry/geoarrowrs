use arrow::array::{Array, BooleanArray, BooleanBuilder, Int32Builder, StringArray, StringBuilder};
use arrow_extendr::IntoArrowRobj;
use extendr_api::prelude::*;
use geo::algorithm::coordinate_position::{CoordPos, CoordinatePosition};
use geo::algorithm::dimensions::{Dimensions, HasDimensions};
use geo::algorithm::relate::{IntersectionMatrix, Relate};
use geo::indexed::PreparedGeometry;
use geo::{Coord, Geometry, LineString, Point, Polygon};
use geoarrow_array::GeoArrowArray;
use rayon::prelude::*;

use crate::threads::with_pool;
use crate::{
    as_geo_geometries, as_geo_geometries_par, as_geometry_chunks, as_recycled_geometries,
    check_recycle_len, try_float_array,
};

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

/// Every relate walk is the same: recycle `y`, then read one DE-9IM matrix per row.
fn relate_map<T: Send>(
    x: Robj,
    y: Robj,
    map: impl Fn(&IntersectionMatrix) -> T + Sync,
) -> extendr_api::Result<Vec<Option<T>>> {
    let threads = crate::threads::Threads::get();
    let geoms = as_geo_geometries_par(threads, &as_geometry_chunks(x)?)?;
    let others = as_recycled_geometries(threads, y, geoms.len(), "y")?;

    let values = match others.as_slice() {
        // one geometry against every row, so it is noded once per task rather than once per row
        [Some(other)] => with_pool(threads, || {
            geoms
                .par_chunks(crate::PAR_MIN_CHUNK)
                .map(|chunk| {
                    let prepared = PreparedGeometry::from(other);
                    chunk
                        .iter()
                        .map(|geom| geom.as_ref().map(|g| map(&g.relate(&prepared))))
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>()
                .into_iter()
                .flatten()
                .collect::<Vec<_>>()
        }),
        _ => with_pool(threads, || {
            geoms
                .par_iter()
                .enumerate()
                .map(
                    |(i, geom)| match (geom, others[i % others.len()].as_ref()) {
                        (Some(g), Some(o)) => Some(map(&g.relate(o))),
                        _ => None,
                    },
                )
                .collect::<Vec<_>>()
        }),
    };

    Ok(values)
}

/// Every binary predicate is the same pairwise walk over one DE-9IM matrix per row.
fn relate_predicate(
    x: Robj,
    y: Robj,
    test: fn(&IntersectionMatrix) -> bool,
) -> extendr_api::Result<Robj> {
    BooleanArray::from(relate_map(x, y, test)?)
        .into_data()
        .into_arrow_robj()
}

/// Test a topological relationship
///
/// Each function compares `x` and `y` row by row and returns `TRUE` when the
/// named DE-9IM relationship holds. These functions recycle `y` against `x`.
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
/// coordinate order, so it still treats two geometries with different windings
/// as equal.
///
/// Each function returns `NA` for a null geometry on either side.
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
/// geometries relates, from which you can derive every named predicate.
///
/// @details
/// Read the string as the intersections of the interior, boundary and exterior
/// of `x` with those of `y`, in that order. Each character is the dimension of
/// that intersection: `F` for empty, `0` for a point, `1` for a curve, and `2`
/// for a surface. `ga_relate()` returns `NA` for a null geometry on either side.
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
    StringArray::from(relate_map(x, y, de9im)?)
        .into_data()
        .into_arrow_robj()
}

/// Exact equality as GEOS defines it: same type, same structure, and every coordinate within `tol` in planar distance.
pub(crate) trait EqualsExact {
    fn equals_exact(&self, other: &Self, tol: f64) -> bool;
}

impl EqualsExact for Coord<f64> {
    fn equals_exact(&self, other: &Self, tol: f64) -> bool {
        (self.x - other.x).hypot(self.y - other.y) <= tol
    }
}

/// Sequences match element by element, in order.
impl<T: EqualsExact> EqualsExact for [T] {
    fn equals_exact(&self, other: &Self, tol: f64) -> bool {
        self.len() == other.len() && self.iter().zip(other).all(|(a, b)| a.equals_exact(b, tol))
    }
}

impl EqualsExact for Point<f64> {
    fn equals_exact(&self, other: &Self, tol: f64) -> bool {
        self.0.equals_exact(&other.0, tol)
    }
}

impl EqualsExact for LineString<f64> {
    fn equals_exact(&self, other: &Self, tol: f64) -> bool {
        self.0.as_slice().equals_exact(other.0.as_slice(), tol)
    }
}

/// Rings match in order, exterior first.
impl EqualsExact for Polygon<f64> {
    fn equals_exact(&self, other: &Self, tol: f64) -> bool {
        self.exterior().equals_exact(other.exterior(), tol)
            && self.interiors().equals_exact(other.interiors(), tol)
    }
}

/// Geometries of different types are never equal.
impl EqualsExact for Geometry<f64> {
    fn equals_exact(&self, other: &Self, tol: f64) -> bool {
        match (self, other) {
            (Geometry::Point(p), Geometry::Point(q)) => p.equals_exact(q, tol),
            (Geometry::LineString(p), Geometry::LineString(q)) => p.equals_exact(q, tol),
            (Geometry::Polygon(p), Geometry::Polygon(q)) => p.equals_exact(q, tol),
            (Geometry::Rect(p), Geometry::Rect(q)) => {
                [p.min(), p.max()][..].equals_exact(&[q.min(), q.max()][..], tol)
            }
            (Geometry::MultiPoint(p), Geometry::MultiPoint(q)) => {
                p.0.as_slice().equals_exact(q.0.as_slice(), tol)
            }
            (Geometry::MultiLineString(p), Geometry::MultiLineString(q)) => {
                p.0.as_slice().equals_exact(q.0.as_slice(), tol)
            }
            (Geometry::MultiPolygon(p), Geometry::MultiPolygon(q)) => {
                p.0.as_slice().equals_exact(q.0.as_slice(), tol)
            }
            (Geometry::GeometryCollection(p), Geometry::GeometryCollection(q)) => {
                p.0.as_slice().equals_exact(q.0.as_slice(), tol)
            }
            _ => false,
        }
    }
}

/// Test whether geometries are identical within a tolerance
///
/// `TRUE` when `x` and `y` have the same type and structure and every
/// coordinate lies within `tolerance` of its counterpart. `ga_equals_exact()`
/// recycles `y` and `tolerance` against `x`.
///
/// @details
/// Unlike `ga_equals_topo()`, `ga_equals_exact()` compares coordinates in
/// order, so it does not treat a line and its reverse as equal. It measures
/// planar distance between coordinates and returns `NA` for a null on either
/// side.
///
/// @param x a GeoArrow geometry array
/// @param y a GeoArrow geometry array; length 1 or the same length as `x`
/// @param tolerance the largest planar distance allowed between matching coordinates; length 1 or the same length as `x`
/// @returns a boolean array of the same length as `x`
/// @export
/// @family topology
/// @examplesIf requireNamespace("sf", quietly = TRUE) && requireNamespace("geoarrow", quietly = TRUE)
/// x <- geoarrow::as_geoarrow_array(sf::st_sfc(
///   sf::st_linestring(rbind(c(0, 0), c(1, 1))),
///   sf::st_linestring(rbind(c(1, 1), c(0, 0)))
/// ))
/// y <- geoarrow::as_geoarrow_array(sf::st_sfc(
///   sf::st_linestring(rbind(c(0, 0), c(1, 1.01)))
/// ))
///
/// # ga_equals_exact() does not treat the reversed line as equal
/// as.vector(ga_equals_exact(x, y, 0.1))
#[extendr]
fn ga_equals_exact(
    x: Robj,
    y: Robj,
    #[extendr(default = "0")] tolerance: Robj,
) -> extendr_api::Result<Robj> {
    let threads = crate::threads::Threads::get();
    let geoms = as_geo_geometries_par(threads, &as_geometry_chunks(x)?)?;
    let others = as_recycled_geometries(threads, y, geoms.len(), "y")?;
    let tol = try_float_array(tolerance, "tolerance")?;
    check_recycle_len(tol.len(), geoms.len(), "tolerance")?;

    let mut bldr = BooleanBuilder::with_capacity(geoms.len());
    for ((a, b), t) in geoms
        .iter()
        .zip(others.iter().cycle())
        .zip(tol.iter().cycle())
    {
        match (a, b, t) {
            (Some(a), Some(b), Some(t)) => bldr.append_value(a.equals_exact(b, t)),
            _ => bldr.append_null(),
        }
    }

    bldr.finish().into_data().into_arrow_robj()
}

/// Determine the topological dimension of geometries
///
/// Returns 0 for points, 1 for lines and curves, and 2 for surfaces.
///
/// @details
/// `ga_dimension()` returns `NA` for an empty geometry, which has no dimension,
/// rather than a point's 0. `ga_boundary_dimension()` gives the dimension of
/// the geometry's boundary instead, so it returns 1 for a polygon and `NA` for
/// a point.
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
/// An empty geometry is distinct from a null one. `ga_is_empty()` returns `NA`
/// for a null geometry.
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
/// point that lies exactly on an edge from one strictly inside.
/// `ga_coordinate_position()` recycles `point` against `geometry` and returns
/// `NA` for a null geometry or a null point.
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
/// p <- ga_point(c(2, 0, 9), c(2, 2, 9))
/// as.vector(ga_coordinate_position(g, p))
#[extendr]
fn ga_coordinate_position(geometry: Robj, point: Robj) -> extendr_api::Result<Robj> {
    let chunks = as_geometry_chunks(geometry)?;
    let n: usize = chunks.iter().map(|c| c.len()).sum();
    let points = as_recycled_geometries(crate::threads::Threads::get(), point, n, "point")?;
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
    fn ga_equals_exact;
    fn ga_dimension;
    fn ga_boundary_dimension;
    fn ga_is_empty;
    fn ga_coordinate_position;
}
