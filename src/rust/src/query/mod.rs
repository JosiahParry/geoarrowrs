use arrow::array::{Array, BooleanBuilder, Float64Builder};
use arrow_extendr::IntoArrowRobj;
use extendr_api::prelude::*;
use geo::algorithm::closest_point::ClosestPoint;
use geo::algorithm::haversine_closest_point::HaversineClosestPoint;
use geo::algorithm::interior_point::InteriorPoint;
use geo::algorithm::is_convex::IsConvex;
use geo::algorithm::line_locate_point::LineLocatePoint;
use geo::{Closest, Geometry, Point};
use geoarrow::array::PointBuilder;
use geoarrow::datatypes::{Dimension, PointType};
use geoarrow_array::GeoArrowArray;

use crate::{as_geo_geometries, as_geometry_chunks, check_recycle_len};

/// Read a geometry argument as one point per row, recycled against `n`.
fn as_geo_points(
    robj: Robj,
    n: usize,
    label: &'static str,
) -> extendr_api::Result<Vec<Option<Point<f64>>>> {
    let chunks = as_geometry_chunks(robj)?;
    let mut out = Vec::new();
    for chunk in &chunks {
        for geom in as_geo_geometries(chunk.as_ref())? {
            out.push(match geom {
                Some(Geometry::Point(p)) => Some(p),
                _ => None,
            });
        }
    }
    check_recycle_len(out.len(), n, label)?;
    Ok(out)
}

/// An indeterminate result has no single closest point, so it becomes a null element.
fn closest_to_option(closest: Closest<f64>) -> Option<Point<f64>> {
    match closest {
        Closest::Intersection(p) | Closest::SinglePoint(p) => Some(p),
        Closest::Indeterminate => None,
    }
}

/// geo implements IsConvex for LineString only, so a polygon is judged on its exterior ring.
fn convex_of(g: &Geometry<f64>) -> Option<bool> {
    match g {
        Geometry::LineString(ls) => Some(ls.is_convex()),
        Geometry::Polygon(p) => Some(p.exterior().is_convex()),
        _ => None,
    }
}

/// geo implements LineLocatePoint for lines and linestrings only.
fn locate_of(g: &Geometry<f64>, p: &Point<f64>) -> Option<f64> {
    match g {
        Geometry::LineString(ls) => ls.line_locate_point(p),
        Geometry::Line(l) => l.line_locate_point(p),
        _ => None,
    }
}

/// Shared body for the two closest point functions, which differ only in their metric.
fn closest_impl(geometry: Robj, point: Robj, haversine: bool) -> extendr_api::Result<Robj> {
    let chunks = as_geometry_chunks(geometry)?;
    let n: usize = chunks.iter().map(|c| c.len()).sum();
    let points = as_geo_points(point, n, "point")?;

    let metadata = chunks[0].data_type().metadata().clone();
    let mut bldr = PointBuilder::new(PointType::new(Dimension::XY, metadata));

    for chunk in &chunks {
        for (geom, p) in as_geo_geometries(chunk.as_ref())?
            .into_iter()
            .zip(points.iter().cycle())
        {
            let found = match (geom, p) {
                (Some(g), Some(p)) => closest_to_option(if haversine {
                    g.haversine_closest_point(p)
                } else {
                    g.closest_point(p)
                }),
                _ => None,
            };
            bldr.try_push_point(found.as_ref())
                .map_err(|e| Error::Other(e.to_string()))?;
        }
    }

    bldr.finish().into_arrow_robj()
}

/// Closest point on a geometry
///
/// Returns the position on each geometry nearest the corresponding point,
/// using planar distance. `ga_closest_point_haversine()` measures on a sphere
/// instead, treating coordinates as longitude and latitude in degrees.
///
/// @details
/// When the point lies on the geometry the intersection itself is returned.
/// A geometry with no single nearest position, such as a point equidistant
/// from both ends of a symmetric line, becomes a null element, as does a null
/// geometry or a null point.
///
/// @param geometry a GeoArrow geometry array
/// @param point a GeoArrow point array; length 1 or the same length as `geometry`
/// @returns a GeoArrow point array of the same length as `geometry`
/// @export
/// @rdname ga_closest_point
/// @family query
/// @references [ClosestPoint](https://docs.rs/geo/latest/geo/algorithm/closest_point/trait.ClosestPoint.html)
/// @examplesIf requireNamespace("sf", quietly = TRUE) && requireNamespace("geoarrow", quietly = TRUE)
/// line <- geoarrow::as_geoarrow_array(sf::st_sfc(
///   sf::st_linestring(cbind(c(0, 10), c(0, 0)))
/// ))
/// pt <- geoarrow::as_geoarrow_array(sf::st_sfc(sf::st_point(c(4, 5))))
///
/// sf::st_as_sfc(geoarrow::as_geoarrow_vctr(ga_closest_point(line, pt)))
#[extendr]
fn ga_closest_point(geometry: Robj, point: Robj) -> extendr_api::Result<Robj> {
    closest_impl(geometry, point, false)
}

/// @export
/// @rdname ga_closest_point
/// @family query
/// @references [HaversineClosestPoint](https://docs.rs/geo/latest/geo/algorithm/haversine_closest_point/trait.HaversineClosestPoint.html)
#[extendr]
fn ga_closest_point_haversine(geometry: Robj, point: Robj) -> extendr_api::Result<Robj> {
    closest_impl(geometry, point, true)
}

/// Compute a representative point inside a geometry
///
/// Returns a point guaranteed to lie on the geometry, unlike [ga_centroid()],
/// which can fall outside a concave shape.
///
/// @details
/// For a polygon this is a point on the interior, chosen from the horizontal
/// line closest to the centroid. An empty or null geometry becomes a null
/// element.
///
/// @param geometry a GeoArrow geometry array
/// @returns a GeoArrow point array of the same length as `geometry`
/// @export
/// @family query
/// @references [InteriorPoint](https://docs.rs/geo/latest/geo/algorithm/interior_point/trait.InteriorPoint.html)
/// @examplesIf requireNamespace("sf", quietly = TRUE) && requireNamespace("geoarrow", quietly = TRUE)
/// g <- geoarrow::as_geoarrow_array(sf::st_sfc(sf::st_polygon(list(
///   matrix(c(0, 0, 4, 0, 4, 4, 0, 4, 0, 0), ncol = 2, byrow = TRUE)
/// ))))
///
/// sf::st_as_sfc(geoarrow::as_geoarrow_vctr(ga_interior_point(g)))
#[extendr]
fn ga_interior_point(geometry: Robj) -> extendr_api::Result<Robj> {
    let chunks = as_geometry_chunks(geometry)?;
    let metadata = chunks[0].data_type().metadata().clone();
    let mut bldr = PointBuilder::new(PointType::new(Dimension::XY, metadata));

    for chunk in &chunks {
        for geom in as_geo_geometries(chunk.as_ref())? {
            let found = geom.and_then(|g| g.interior_point());
            bldr.try_push_point(found.as_ref())
                .map_err(|e| Error::Other(e.to_string()))?;
        }
    }

    bldr.finish().into_arrow_robj()
}

/// Test whether a ring is convex
///
/// `TRUE` when the geometry's ring turns consistently in one direction. A
/// polygon is judged on its exterior ring.
///
/// @details
/// A linestring must be closed, that is form a ring, to be reported convex; an
/// open one is `FALSE`. A polygon's exterior ring is always closed, so polygons
/// need no special handling. Any other geometry type, or a null geometry,
/// becomes `NA` rather than `FALSE`.
///
/// @param geometry a GeoArrow linestring or polygon array
/// @returns a boolean array of the same length as `geometry`
/// @export
/// @family query
/// @references [IsConvex](https://docs.rs/geo/latest/geo/algorithm/is_convex/trait.IsConvex.html)
/// @examplesIf requireNamespace("sf", quietly = TRUE) && requireNamespace("geoarrow", quietly = TRUE)
/// square <- sf::st_polygon(list(
///   matrix(c(0, 0, 1, 0, 1, 1, 0, 1, 0, 0), ncol = 2, byrow = TRUE)
/// ))
/// g <- geoarrow::as_geoarrow_array(sf::st_sfc(square))
///
/// as.vector(ga_is_convex(g))
#[extendr]
fn ga_is_convex(geometry: Robj) -> extendr_api::Result<Robj> {
    let chunks = as_geometry_chunks(geometry)?;
    let n = chunks.iter().map(|c| c.len()).sum();
    let mut bldr = BooleanBuilder::with_capacity(n);

    for chunk in &chunks {
        for geom in as_geo_geometries(chunk.as_ref())? {
            match geom.as_ref().and_then(convex_of) {
                Some(v) => bldr.append_value(v),
                None => bldr.append_null(),
            }
        }
    }

    bldr.finish().into_data().into_arrow_robj()
}

/// Locate a point along a line
///
/// Returns how far along each line the closest position to the corresponding
/// point lies, as a fraction between 0 and 1.
///
/// @details
/// 0 is the start of the line and 1 its end, so the value multiplied by the
/// line's length gives a distance. Only lines and linestrings can be located
/// along; any other geometry type, a null geometry, a null point, or a
/// zero length line becomes `NA`.
///
/// @param geometry a GeoArrow linestring array
/// @param point a GeoArrow point array; length 1 or the same length as `geometry`
/// @returns a double array of the same length as `geometry`
/// @export
/// @family query
/// @references [LineLocatePoint](https://docs.rs/geo/latest/geo/algorithm/line_locate_point/trait.LineLocatePoint.html)
/// @examplesIf requireNamespace("sf", quietly = TRUE) && requireNamespace("geoarrow", quietly = TRUE)
/// line <- geoarrow::as_geoarrow_array(sf::st_sfc(
///   sf::st_linestring(cbind(c(0, 10), c(0, 0)))
/// ))
/// pt <- geoarrow::as_geoarrow_array(sf::st_sfc(sf::st_point(c(2.5, 0))))
///
/// as.vector(ga_line_locate_point(line, pt))
#[extendr]
fn ga_line_locate_point(geometry: Robj, point: Robj) -> extendr_api::Result<Robj> {
    let chunks = as_geometry_chunks(geometry)?;
    let n: usize = chunks.iter().map(|c| c.len()).sum();
    let points = as_geo_points(point, n, "point")?;
    let mut bldr = Float64Builder::with_capacity(n);

    for chunk in &chunks {
        for (geom, p) in as_geo_geometries(chunk.as_ref())?
            .into_iter()
            .zip(points.iter().cycle())
        {
            let found = match (geom, p) {
                (Some(g), Some(p)) => locate_of(&g, p),
                _ => None,
            };
            match found {
                Some(v) => bldr.append_value(v),
                None => bldr.append_null(),
            }
        }
    }

    bldr.finish().into_arrow_robj()
}

extendr_module! {
    mod query;
    fn ga_closest_point;
    fn ga_closest_point_haversine;
    fn ga_interior_point;
    fn ga_is_convex;
    fn ga_line_locate_point;
}
