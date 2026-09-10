use arrow::array::{Array, Int32Builder};
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
/// vertices in order. The output has the same length as the input.
///
/// @details
/// `exterior_coords()` skips the interior rings of a polygon, so it gives the
/// outline only. Both preserve the order the coordinates are stored in, so a
/// closed ring repeats its first vertex at the end.
///
/// A null geometry stays null.
///
/// @param geometry a GeoArrow geometry array
/// @returns a GeoArrow multipoint array of the same length as `geometry`
/// @export
/// @rdname coords
/// @family iteration
/// @references [CoordsIter](https://docs.rs/geo/latest/geo/algorithm/coords_iter/trait.CoordsIter.html)
/// @examplesIf requireNamespace("sf", quietly = TRUE) && requireNamespace("geoarrow", quietly = TRUE)
/// ring <- matrix(c(0, 0, 2, 0, 2, 2, 0, 2, 0, 0), ncol = 2, byrow = TRUE)
/// g <- geoarrow::as_geoarrow_array(sf::st_sfc(sf::st_polygon(list(ring))))
///
/// sf::st_as_sfc(geoarrow::as_geoarrow_vctr(coords(g)))
/// as.vector(nanoarrow::convert_array(n_coords(g)))
#[extendr]
fn coords(geometry: Robj) -> anyhow::Result<Robj> {
    coords_impl(geometry, false)
}

/// @export
/// @rdname coords
/// @family iteration
#[extendr]
fn exterior_coords(geometry: Robj) -> anyhow::Result<Robj> {
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
/// Counts every coordinate that [coords()] would return, so a closed ring
/// counts its repeated final vertex. A null geometry gives `NA`.
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
/// as.vector(nanoarrow::convert_array(n_coords(g)))
#[extendr]
fn n_coords(geometry: Robj) -> anyhow::Result<Robj> {
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
/// geometry's two point segments. The output has the same length as the input.
///
/// @details
/// A polygon contributes the segments of every ring, interior rings included.
/// A point has no segments and becomes a null element, as does a null
/// geometry.
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
/// lengths(sf::st_as_sfc(geoarrow::as_geoarrow_vctr(lines(g))))
#[extendr]
fn lines(geometry: Robj) -> anyhow::Result<Robj> {
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

extendr_module! {
    mod iteration;
    fn coords;
    fn exterior_coords;
    fn n_coords;
    fn lines;
}
