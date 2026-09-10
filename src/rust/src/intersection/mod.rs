use arrow_extendr::IntoArrowRobj;
use extendr_api::prelude::*;
use geo::algorithm::line_intersection::{
    LineIntersection, line_intersection as geo_line_intersection,
};
use geo::algorithm::lines_iter::LinesIter;
use geo::sweep::Intersections;
use geo::{Geometry, Line, MultiPoint, Point};
use geoarrow::array::MultiPointBuilder;
use geoarrow::datatypes::{Dimension, MultiPointType};
use geoarrow_array::GeoArrowArray;

use crate::{as_geo_geometries, as_geometry_chunks, check_recycle_len};

/// Read a geometry as the single segment the line functions need.
fn as_line(g: &Geometry<f64>) -> Option<Line<f64>> {
    match g {
        Geometry::Line(l) => Some(*l),
        Geometry::LineString(ls) => {
            let coords = &ls.0;
            if coords.len() == 2 {
                Some(Line::new(coords[0], coords[1]))
            } else {
                None
            }
        }
        _ => None,
    }
}

/// Collect every segment of a geometry, so the sweep has something to cross.
fn segments_of(g: &Geometry<f64>) -> Option<Vec<Line<f64>>> {
    match g {
        Geometry::Line(v) => Some(vec![*v]),
        Geometry::LineString(v) => Some(v.lines_iter().collect()),
        Geometry::MultiLineString(v) => Some(v.lines_iter().collect()),
        Geometry::Polygon(v) => Some(v.lines_iter().collect()),
        Geometry::MultiPolygon(v) => Some(v.lines_iter().collect()),
        Geometry::Rect(v) => Some(v.lines_iter().collect()),
        Geometry::Triangle(v) => Some(v.lines_iter().collect()),
        _ => None,
    }
}

/// Intersect pairs of two point lines
///
/// Returns where each pair of lines meets: a point when they cross, and a line
/// when they overlap along a shared stretch. `y` is recycled against `x`.
///
/// @details
/// The answer is a point when the lines cross and a segment when they overlap.
/// Both come back as a multipoint so the column has one type: a crossing gives
/// one point, an overlap gives the two endpoints of the shared stretch. So
/// `n_coords()` tells the two apart, and an overlap can be rebuilt from its
/// endpoints.
///
/// Both arguments must hold single segments, that is a `LINE` or a two point
/// `LINESTRING`. Longer linestrings, other geometry types, and null rows come
/// back null, as do pairs that simply do not meet. Use
/// [self_intersections()] for a geometry with many segments.
///
/// @param x a GeoArrow array of two point linestrings
/// @param y a GeoArrow array of two point linestrings; length 1 or the same
///   length as `x`
/// @returns a GeoArrow multipoint array of the same length as `x`
/// @export
/// @family intersection
/// @references [line_intersection](https://docs.rs/geo/latest/geo/algorithm/line_intersection/fn.line_intersection.html)
/// @examplesIf requireNamespace("sf", quietly = TRUE) && requireNamespace("geoarrow", quietly = TRUE)
/// a <- sf::st_linestring(cbind(c(0, 2), c(0, 2)))
/// b <- sf::st_linestring(cbind(c(0, 2), c(2, 0)))
/// x <- geoarrow::as_geoarrow_array(sf::st_sfc(a))
/// y <- geoarrow::as_geoarrow_array(sf::st_sfc(b))
///
/// sf::st_as_sfc(geoarrow::as_geoarrow_vctr(line_intersection(x, y)))
#[extendr]
fn line_intersection(x: Robj, y: Robj) -> anyhow::Result<Robj> {
    let chunks = as_geometry_chunks(x).map_err(|e| anyhow::anyhow!("{e}"))?;
    let n: usize = chunks.iter().map(|c| c.len()).sum();

    let other_chunks = as_geometry_chunks(y).map_err(|e| anyhow::anyhow!("{e}"))?;
    let mut others: Vec<Option<Line<f64>>> = Vec::new();
    for chunk in &other_chunks {
        let geoms = as_geo_geometries(chunk.as_ref()).map_err(|e| anyhow::anyhow!("{e}"))?;
        for geom in geoms {
            others.push(geom.as_ref().and_then(as_line));
        }
    }
    check_recycle_len(others.len(), n, "y").map_err(|e| anyhow::anyhow!("{e}"))?;

    let metadata = chunks[0].data_type().metadata().clone();
    let mut bldr = MultiPointBuilder::new(MultiPointType::new(Dimension::XY, metadata));

    for chunk in &chunks {
        let geoms = as_geo_geometries(chunk.as_ref()).map_err(|e| anyhow::anyhow!("{e}"))?;
        for (geom, other) in geoms.into_iter().zip(others.iter().cycle()) {
            let hit = match (geom.as_ref().and_then(as_line), other) {
                (Some(p), Some(q)) => geo_line_intersection(p, *q),
                _ => None,
            };
            // one point for a crossing, two for an overlap, so the column has one type
            let result = hit.map(|found| match found {
                LineIntersection::SinglePoint { intersection, .. } => {
                    MultiPoint(vec![Point::from(intersection)])
                }
                LineIntersection::Collinear { intersection } => MultiPoint(vec![
                    Point::from(intersection.start),
                    Point::from(intersection.end),
                ]),
            });
            bldr.push_multi_point(result.as_ref())
                .map_err(|e| anyhow::anyhow!("{e}"))?;
        }
    }

    bldr.finish()
        .into_arrow_robj()
        .map_err(|e| anyhow::anyhow!("{e}"))
}

/// Find where a geometry crosses itself
///
/// Returns the points at which a geometry's own segments intersect, one
/// multipoint per input geometry.
///
/// @details
/// Uses the Bentley-Ottmann sweep line, which finds all crossings in roughly
/// `n log n` rather than by testing every pair. This is how to locate the
/// problem that [is_valid()] reports: a self intersecting polygon comes back
/// with the offending points.
///
/// Segments that merely share an endpoint, as consecutive segments of a
/// linestring always do, are not counted. A geometry with no crossings gives
/// an empty multipoint rather than a null, so an empty result is
/// distinguishable from an unsupported geometry. Points and null rows come
/// back null.
///
/// @param geometry a GeoArrow geometry array
/// @returns a GeoArrow multipoint array of the same length as `geometry`
/// @export
/// @family intersection
/// @references [Intersections](https://docs.rs/geo/latest/geo/sweep/struct.Intersections.html)
/// @examplesIf requireNamespace("sf", quietly = TRUE) && requireNamespace("geoarrow", quietly = TRUE)
/// bowtie <- sf::st_polygon(list(matrix(
///   c(0, 0, 2, 2, 2, 0, 0, 2, 0, 0), ncol = 2, byrow = TRUE
/// )))
/// g <- geoarrow::as_geoarrow_array(sf::st_sfc(bowtie))
///
/// as.vector(nanoarrow::convert_array(is_valid(g)))
/// sf::st_as_sfc(geoarrow::as_geoarrow_vctr(self_intersections(g)))
#[extendr]
fn self_intersections(geometry: Robj) -> anyhow::Result<Robj> {
    let chunks = as_geometry_chunks(geometry).map_err(|e| anyhow::anyhow!("{e}"))?;
    let metadata = chunks[0].data_type().metadata().clone();
    let mut bldr = MultiPointBuilder::new(MultiPointType::new(Dimension::XY, metadata));

    for chunk in &chunks {
        let geoms = as_geo_geometries(chunk.as_ref()).map_err(|e| anyhow::anyhow!("{e}"))?;
        for geom in geoms {
            let points = geom.as_ref().and_then(segments_of).map(|segments| {
                let mut found: Vec<Point<f64>> = Vec::new();
                for (_, _, hit) in Intersections::from_iter(segments) {
                    match hit {
                        LineIntersection::SinglePoint {
                            intersection,
                            is_proper: true,
                        } => found.push(Point::from(intersection)),
                        LineIntersection::Collinear { intersection } => {
                            found.push(Point::from(intersection.start));
                            found.push(Point::from(intersection.end));
                        }
                        _ => {}
                    }
                }
                MultiPoint(found)
            });
            bldr.push_multi_point(points.as_ref())
                .map_err(|e| anyhow::anyhow!("{e}"))?;
        }
    }

    bldr.finish()
        .into_arrow_robj()
        .map_err(|e| anyhow::anyhow!("{e}"))
}

extendr_module! {
    mod intersection;
    fn line_intersection;
    fn self_intersections;
}
