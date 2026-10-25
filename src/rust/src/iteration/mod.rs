use arrow::array::{Array, Float64Builder, Int32Builder};
use arrow_extendr::IntoArrowRobj;
use extendr_api::prelude::*;
use geo::algorithm::coords_iter::CoordsIter;
use geo::algorithm::lines_iter::LinesIter;
use geo::{Geometry, Line, LineString, MultiLineString, MultiPoint, Point};
use geoarrow::array::{MultiLineStringBuilder, MultiPointBuilder};
use geoarrow::datatypes::{Dimension, MultiLineStringType, MultiPointType};
use geoarrow_array::GeoArrowArray;

use crate::{as_geo_geometries, as_geometry_chunks};

/// geo implements LinesIter for the concrete types but not for Geometry, so dispatch.
fn lines_of(g: &Geometry<f64>) -> Option<Vec<Line<f64>>> {
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

/// Collect a geometry's coordinates as points
///
/// Returns one multipoint per input geometry, holding that geometry's
/// vertices in order. Each function keeps the input's length.
///
/// @details
/// `ga_exterior_coords()` skips the interior rings of a polygon, so it gives the
/// outline only. Both preserve the coordinates' storage order, so they keep a
/// closed ring's repeated first vertex at the end.
///
/// Each function returns null for a null geometry.
///
/// @param geometry a GeoArrow geometry array
/// @returns a GeoArrow multipoint array of the same length as `geometry`
/// @export
/// @rdname ga_coords
/// @family iteration
/// @references [CoordsIter](https://docs.rs/geo/latest/geo/algorithm/coords_iter/trait.CoordsIter.html)
/// @examplesIf requireNamespace("sf", quietly = TRUE) && requireNamespace("geoarrow", quietly = TRUE)
/// ring <- matrix(c(0, 0, 2, 0, 2, 2, 0, 2, 0, 0), ncol = 2, byrow = TRUE)
/// g <- geoarrow::as_geoarrow_array(sf::st_sfc(sf::st_polygon(list(ring))))
///
/// sf::st_as_sfc(geoarrow::as_geoarrow_vctr(ga_coords(g)))
/// as.vector(ga_n_coords(g))
#[extendr]
fn ga_coords(geometry: Robj) -> anyhow::Result<Robj> {
    coords_impl(geometry, false)
}

/// @export
/// @rdname ga_coords
/// @family iteration
#[extendr]
fn ga_exterior_coords(geometry: Robj) -> anyhow::Result<Robj> {
    coords_impl(geometry, true)
}

/// Shared body for the two coordinate collectors, which differ only in which rings they walk.
fn coords_impl(geometry: Robj, exterior_only: bool) -> anyhow::Result<Robj> {
    let chunks = as_geometry_chunks(geometry).map_err(|e| anyhow::anyhow!("{e}"))?;
    let metadata = chunks[0].data_type().metadata().clone();
    let mut bldr = MultiPointBuilder::new(MultiPointType::new(Dimension::XY, metadata));

    for chunk in &chunks {
        let geoms = as_geo_geometries(chunk.as_ref()).map_err(|e| anyhow::anyhow!("{e}"))?;
        for geom in geoms {
            let points = geom.map(|g| {
                let coords: Vec<Point<f64>> = if exterior_only {
                    g.exterior_coords_iter().map(Point::from).collect()
                } else {
                    g.coords_iter().map(Point::from).collect()
                };
                MultiPoint(coords)
            });
            bldr.push_multi_point(points.as_ref())
                .map_err(|e| anyhow::anyhow!("{e}"))?;
        }
    }

    bldr.finish()
        .into_arrow_robj()
        .map_err(|e| anyhow::anyhow!("{e}"))
}

/// Count the coordinates in each geometry
///
/// Returns how many vertices each geometry holds.
///
/// @details
/// Counts every coordinate that [ga_coords()] would return, so it counts the
/// repeated final vertex of a closed ring. `ga_n_coords()` returns `NA` for a
/// null geometry.
///
/// @param geometry a GeoArrow geometry array
/// @returns an integer array of the same length as `geometry`
/// @export
/// @family iteration
/// @references [CoordsIter](https://docs.rs/geo/latest/geo/algorithm/coords_iter/trait.CoordsIter.html)
/// @examplesIf requireNamespace("sf", quietly = TRUE) && requireNamespace("geoarrow", quietly = TRUE)
/// ring <- matrix(c(0, 0, 2, 0, 2, 2, 0, 2, 0, 0), ncol = 2, byrow = TRUE)
/// g <- geoarrow::as_geoarrow_array(sf::st_sfc(sf::st_polygon(list(ring))))
///
/// as.vector(ga_n_coords(g))
#[extendr]
fn ga_n_coords(geometry: Robj) -> anyhow::Result<Robj> {
    let chunks = as_geometry_chunks(geometry).map_err(|e| anyhow::anyhow!("{e}"))?;
    let n = chunks.iter().map(|c| c.len()).sum();
    let mut bldr = Int32Builder::with_capacity(n);

    for chunk in &chunks {
        let geoms = as_geo_geometries(chunk.as_ref()).map_err(|e| anyhow::anyhow!("{e}"))?;
        for geom in geoms {
            match geom {
                Some(g) => bldr.append_value(g.coords_count() as i32),
                None => bldr.append_null(),
            }
        }
    }

    bldr.finish()
        .into_data()
        .into_arrow_robj()
        .map_err(|e| anyhow::anyhow!("{e}"))
}

/// Split geometries into their line segments
///
/// Returns one multilinestring per input geometry, whose parts are that
/// geometry's two point segments. `ga_lines()` keeps the input's length.
///
/// @details
/// For a polygon, `ga_lines()` returns the segments of every ring, including
/// interior rings. It returns a null element for a point, which has no
/// segments, and for a null geometry.
///
/// @param geometry a GeoArrow geometry array
/// @returns a GeoArrow multilinestring array of the same length as `geometry`
/// @export
/// @family iteration
/// @references [LinesIter](https://docs.rs/geo/latest/geo/algorithm/lines_iter/trait.LinesIter.html)
/// @examplesIf requireNamespace("sf", quietly = TRUE) && requireNamespace("geoarrow", quietly = TRUE)
/// ring <- matrix(c(0, 0, 2, 0, 2, 2, 0, 2, 0, 0), ncol = 2, byrow = TRUE)
/// g <- geoarrow::as_geoarrow_array(sf::st_sfc(sf::st_polygon(list(ring))))
///
/// lengths(sf::st_as_sfc(geoarrow::as_geoarrow_vctr(ga_lines(g))))
#[extendr]
fn ga_lines(geometry: Robj) -> anyhow::Result<Robj> {
    let chunks = as_geometry_chunks(geometry).map_err(|e| anyhow::anyhow!("{e}"))?;
    let metadata = chunks[0].data_type().metadata().clone();
    let mut bldr = MultiLineStringBuilder::new(MultiLineStringType::new(Dimension::XY, metadata));

    for chunk in &chunks {
        let geoms = as_geo_geometries(chunk.as_ref()).map_err(|e| anyhow::anyhow!("{e}"))?;
        for geom in geoms {
            let segments = geom.as_ref().and_then(lines_of).map(|ls| {
                MultiLineString(
                    ls.into_iter()
                        .map(|l| LineString::new(vec![l.start, l.end]))
                        .collect(),
                )
            });
            bldr.push_multi_line_string(segments.as_ref())
                .map_err(|e| anyhow::anyhow!("{e}"))?;
        }
    }

    bldr.finish()
        .into_arrow_robj()
        .map_err(|e| anyhow::anyhow!("{e}"))
}

/// Shared body for the two ordinate readers, which differ only in the axis taken.
fn ordinate_impl(geometry: Robj, y_axis: bool) -> anyhow::Result<Robj> {
    let chunks = as_geometry_chunks(geometry).map_err(|e| anyhow::anyhow!("{e}"))?;
    let n = chunks.iter().map(|c| c.len()).sum();
    let mut bldr = Float64Builder::with_capacity(n);

    for chunk in &chunks {
        let geoms = as_geo_geometries(chunk.as_ref()).map_err(|e| anyhow::anyhow!("{e}"))?;
        for geom in geoms {
            match geom {
                Some(Geometry::Point(p)) if y_axis => bldr.append_value(p.y()),
                Some(Geometry::Point(p)) => bldr.append_value(p.x()),
                _ => bldr.append_null(),
            }
        }
    }

    bldr.finish()
        .into_arrow_robj()
        .map_err(|e| anyhow::anyhow!("{e}"))
}

/// Read the x or y of a point
///
/// Returns one coordinate per row. Each function returns `NA` for any geometry
/// that is not a single point, and for any null or empty point.
///
/// @details
/// Both functions accept either coordinate encoding and return the same values
/// whether the point array holds a struct of two double columns or interleaved
/// values.
///
/// Use [ga_coords()] for the vertices of a line or polygon, which returns a
/// multipoint per row rather than a single number.
///
/// @param geometry a GeoArrow point array
/// @returns a double array of the same length as `geometry`
/// @export
/// @rdname ga_x
/// @family iteration
/// @examplesIf requireNamespace("geoarrow", quietly = TRUE)
/// pts <- ga_point(c(-111.76, -112.07), c(34.87, 33.45))
///
/// as.vector(ga_x(pts))
/// as.vector(ga_y(pts))
#[extendr]
fn ga_x(geometry: Robj) -> anyhow::Result<Robj> {
    ordinate_impl(geometry, false)
}

/// @export
/// @rdname ga_x
/// @family iteration
#[extendr]
fn ga_y(geometry: Robj) -> anyhow::Result<Robj> {
    ordinate_impl(geometry, true)
}

extendr_module! {
    mod iteration;
    fn ga_coords;
    fn ga_exterior_coords;
    fn ga_n_coords;
    fn ga_lines;
    fn ga_x;
    fn ga_y;
}
