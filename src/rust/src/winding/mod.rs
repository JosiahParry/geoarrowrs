use arrow::array::{Array, BooleanBuilder, StringBuilder};
use arrow_extendr::IntoArrowRobj;
use extendr_api::prelude::*;
use geo::algorithm::orient::{Direction, Orient};
use geo::algorithm::winding_order::WindingOrder;
use geo::{
    Geometry, GeometryCollection, Line, LineString, MultiLineString, MultiPolygon, Polygon, Winding,
};
use geo_traits::to_geo::{ToGeoMultiPolygon, ToGeoPolygon};
use geoarrow::array::{MultiPolygonBuilder, PolygonBuilder};
use geoarrow::datatypes::{Dimension, GeoArrowType, MultiPolygonType, PolygonType};
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor};

use crate::cast::{CastGeoArrow, concat_cast};
use crate::threads::Threads;
use crate::{
    as_geo_geometries, as_geo_geometries_par, as_geometry_chunks, as_multipolygon_chunks,
    as_polygon_chunks,
};

/// Parse the winding direction accepted by `ga_orient()`.
fn parse_direction(direction: &str) -> extendr_api::Result<Direction> {
    match direction {
        "default" | "ccw" => Ok(Direction::Default),
        "reversed" | "cw" => Ok(Direction::Reversed),
        other => Err(Error::Other(format!(
            "`direction` must be one of \"default\", \"ccw\", \"reversed\", or \"cw\", got \"{other}\""
        ))),
    }
}

/// A multi part geometry has one winding only when every part agrees, so fold and bail on a mismatch.
fn agreed_order(mut parts: impl Iterator<Item = Option<WindingOrder>>) -> Option<WindingOrder> {
    let first = parts.next()??;
    for part in parts {
        if part? != first {
            return None;
        }
    }
    Some(first)
}

/// A polygon's winding is that of its exterior ring; geo implements Winding for LineString only.
fn order_of(g: &Geometry<f64>) -> Option<WindingOrder> {
    match g {
        Geometry::LineString(ls) => ls.winding_order(),
        Geometry::Polygon(p) => p.exterior().winding_order(),
        Geometry::MultiLineString(mls) => agreed_order(mls.iter().map(|ls| ls.winding_order())),
        Geometry::MultiPolygon(mp) => agreed_order(mp.iter().map(|p| p.exterior().winding_order())),
        _ => None,
    }
}

/// Apply a winding direction to polygon rings
///
/// Rewinds each polygon so its exterior and interior rings follow a
/// consistent direction. `ga_orient()` keeps the geometry type of the input.
///
/// @details
/// With `"default"`, `ga_orient()` winds the exterior ring counter-clockwise
/// and the interior rings clockwise, which is the winding the OGC simple
/// features and GeoJSON specifications call for. With `"reversed"` it does the
/// opposite. It accepts `"ccw"` and `"cw"` as aliases, which refer to the
/// exterior ring.
///
/// `ga_orient()` does not change which points a polygon covers, only the order
/// of its coordinates. It keeps a null geometry null.
///
/// @param geometry a GeoArrow polygon or multipolygon array
/// @param direction one of `"default"`, `"reversed"`, `"ccw"`, or `"cw"`
/// @returns a GeoArrow array of the same geometry type as `geometry`
/// @export
/// @family winding
/// @references [Orient](https://docs.rs/geo/latest/geo/algorithm/orient/trait.Orient.html)
/// @examplesIf requireNamespace("sf", quietly = TRUE) && requireNamespace("geoarrow", quietly = TRUE)
/// # a clockwise square
/// ring <- rbind(c(0, 0), c(0, 1), c(1, 1), c(1, 0), c(0, 0))
/// p <- sf::st_polygon(list(ring))
/// g <- geoarrow::as_geoarrow_array(sf::st_sfc(p))
///
/// ga_winding_order(g)
/// ga_winding_order(ga_orient(g, "default"))
#[extendr]
fn ga_orient(
    geometry: Robj,
    #[extendr(default = "\"default\"")] direction: &str,
) -> extendr_api::Result<Robj> {
    let dir = parse_direction(direction)?;

    if let Ok(chunks) = as_polygon_chunks(geometry.clone()) {
        let metadata = chunks[0].data_type().metadata().clone();
        let mut bldr = PolygonBuilder::new(PolygonType::new(Dimension::XY, metadata));
        for chunk in &chunks {
            for geom in chunk.iter() {
                if let Some(Ok(g)) = geom {
                    bldr.push_polygon(Some(&g.to_polygon().orient(dir)))
                        .map_err(|e| Error::Other(e.to_string()))?;
                } else {
                    bldr.push_polygon(None::<&Polygon<f64>>)
                        .map_err(|e| Error::Other(e.to_string()))?;
                }
            }
        }
        return bldr.finish().into_arrow_robj();
    }

    if let Ok(chunks) = as_multipolygon_chunks(geometry) {
        let metadata = chunks[0].data_type().metadata().clone();
        let mut bldr = MultiPolygonBuilder::new(MultiPolygonType::new(Dimension::XY, metadata));
        for chunk in &chunks {
            for geom in chunk.iter() {
                if let Some(Ok(g)) = geom {
                    bldr.push_multi_polygon(Some(&g.to_multi_polygon().orient(dir)))
                        .map_err(|e| Error::Other(e.to_string()))?;
                } else {
                    bldr.push_multi_polygon(None::<&MultiPolygon<f64>>)
                        .map_err(|e| Error::Other(e.to_string()))?;
                }
            }
        }
        return bldr.finish().into_arrow_robj();
    }

    Err(Error::Other(
        "Expected polygons or multipolygons. Use cast_geometry() to convert.".to_string(),
    ))
}

/// Determine the winding order of a ring
///
/// Returns `"clockwise"` or `"counterclockwise"` for each geometry. For a
/// polygon, `ga_winding_order()` reports the winding of the exterior ring.
///
/// @details
/// `ga_winding_order()` reports a winding for a multipolygon only when every
/// part's exterior ring agrees, and for a multilinestring only when every part
/// agrees; it returns `NA` for a geometry whose parts disagree, since that
/// geometry has no single winding. It also returns `NA` for any other geometry
/// type, a null geometry, or a ring with fewer than three distinct points.
///
/// @param geometry a GeoArrow linestring, polygon, or multi part array of either
/// @returns a string array of the same length as `geometry`
/// @export
/// @family winding
/// @references [Winding](https://docs.rs/geo/latest/geo/algorithm/winding_order/trait.Winding.html)
/// @examplesIf requireNamespace("sf", quietly = TRUE) && requireNamespace("geoarrow", quietly = TRUE)
/// ring <- matrix(c(0, 0, 0, 1, 1, 1, 1, 0, 0, 0), ncol = 2, byrow = TRUE)
/// g <- geoarrow::as_geoarrow_array(sf::st_sfc(sf::st_polygon(list(ring))))
///
/// as.vector(ga_winding_order(g))
#[extendr]
fn ga_winding_order(geometry: Robj) -> extendr_api::Result<Robj> {
    let chunks = as_geometry_chunks(geometry)?;
    let mut bldr = StringBuilder::new();

    for chunk in &chunks {
        for geom in as_geo_geometries(chunk.as_ref())? {
            match geom.as_ref().and_then(order_of) {
                Some(WindingOrder::Clockwise) => bldr.append_value("clockwise"),
                Some(WindingOrder::CounterClockwise) => bldr.append_value("counterclockwise"),
                None => bldr.append_null(),
            }
        }
    }

    bldr.finish().into_data().into_arrow_robj()
}

/// Test the winding order of a ring
///
/// `ga_is_ccw()` is `TRUE` for counter-clockwise geometries and `ga_is_cw()` is
/// `TRUE` for clockwise ones. Each function tests a polygon on its exterior
/// ring.
///
/// @details
/// Each function returns `NA` rather than `FALSE` for any geometry with no
/// winding order, such as a point or a null geometry, so the two functions are
/// not simply negations of one another.
///
/// @param geometry a GeoArrow linestring or polygon array
/// @returns a boolean array of the same length as `geometry`
/// @export
/// @rdname ga_is_ccw
/// @family winding
/// @references [Winding](https://docs.rs/geo/latest/geo/algorithm/winding_order/trait.Winding.html)
/// @examplesIf requireNamespace("sf", quietly = TRUE) && requireNamespace("geoarrow", quietly = TRUE)
/// ring <- matrix(c(0, 0, 0, 1, 1, 1, 1, 0, 0, 0), ncol = 2, byrow = TRUE)
/// g <- geoarrow::as_geoarrow_array(sf::st_sfc(sf::st_polygon(list(ring))))
///
/// as.vector(ga_is_ccw(g))
/// as.vector(ga_is_cw(ga_orient(g, "reversed")))
#[extendr]
fn ga_is_ccw(geometry: Robj) -> extendr_api::Result<Robj> {
    winding_is(geometry, WindingOrder::CounterClockwise)
}

/// @export
/// @rdname ga_is_ccw
/// @family winding
#[extendr]
fn ga_is_cw(geometry: Robj) -> extendr_api::Result<Robj> {
    winding_is(geometry, WindingOrder::Clockwise)
}

/// Shared body for `ga_is_ccw()` and `ga_is_cw()`, which differ only in the order they test for.
fn winding_is(geometry: Robj, want: WindingOrder) -> extendr_api::Result<Robj> {
    let chunks = as_geometry_chunks(geometry)?;
    let n = chunks.iter().map(|c| c.len()).sum();
    let mut bldr = BooleanBuilder::with_capacity(n);

    for chunk in &chunks {
        for geom in as_geo_geometries(chunk.as_ref())? {
            match geom.as_ref().and_then(order_of) {
                Some(order) => bldr.append_value(order == want),
                None => bldr.append_null(),
            }
        }
    }

    bldr.finish().into_data().into_arrow_robj()
}

/// Reverse coordinate order as GEOS does: every line and ring is reversed, the order of parts and points is not.
trait Reverse {
    fn reversed(self) -> Self;
}

impl Reverse for LineString<f64> {
    fn reversed(mut self) -> Self {
        self.0.reverse();
        self
    }
}

/// The exterior stays first.
impl Reverse for Polygon<f64> {
    fn reversed(self) -> Self {
        let (exterior, interiors) = self.into_inner();
        Polygon::new(
            exterior.reversed(),
            interiors.into_iter().map(Reverse::reversed).collect(),
        )
    }
}

impl Reverse for Geometry<f64> {
    fn reversed(self) -> Self {
        match self {
            Geometry::Line(l) => Geometry::Line(Line::new(l.end, l.start)),
            Geometry::LineString(ls) => Geometry::LineString(ls.reversed()),
            Geometry::Polygon(p) => Geometry::Polygon(p.reversed()),
            Geometry::MultiLineString(m) => Geometry::MultiLineString(MultiLineString(
                m.0.into_iter().map(Reverse::reversed).collect(),
            )),
            Geometry::MultiPolygon(m) => Geometry::MultiPolygon(MultiPolygon(
                m.0.into_iter().map(Reverse::reversed).collect(),
            )),
            Geometry::GeometryCollection(gc) => Geometry::GeometryCollection(GeometryCollection(
                gc.0.into_iter().map(Reverse::reversed).collect(),
            )),
            other => other,
        }
    }
}

/// Reverse the coordinate order of geometries
///
/// Reverses the order of the coordinates in every line and ring, keeping the
/// geometry type of the input. `ga_reverse()` leaves the order of the parts of
/// a multi part geometry unchanged.
///
/// @details
/// `ga_reverse()` returns points and multipoints, which have no order to
/// reverse, as they are. It flips a polygon's winding; use `ga_orient()` to set
/// a winding direction instead. It keeps a null geometry null.
///
/// @param geometry a GeoArrow geometry array
/// @returns a GeoArrow array of the same geometry type as `geometry`
/// @export
/// @family winding
/// @examplesIf requireNamespace("sf", quietly = TRUE) && requireNamespace("geoarrow", quietly = TRUE)
/// g <- geoarrow::as_geoarrow_array(sf::st_sfc(
///   sf::st_linestring(rbind(c(0, 0), c(1, 1), c(2, 0)))
/// ))
///
/// sf::st_as_sfc(geoarrow::as_geoarrow_vctr(ga_reverse(g)))
#[extendr]
fn ga_reverse(geometry: Robj) -> extendr_api::Result<Robj> {
    let chunks = as_geometry_chunks(geometry)?;
    let to_type = chunks[0].data_type();

    // passing points through untouched keeps an empty point empty rather than null
    if matches!(
        to_type,
        GeoArrowType::Point(_) | GeoArrowType::MultiPoint(_)
    ) {
        return concat_cast(chunks, &to_type);
    }

    let geoms = as_geo_geometries_par(Threads::get(), &chunks)?
        .into_iter()
        .map(|g| g.map(Reverse::reversed))
        .collect::<Vec<_>>();

    geoms.cast_geoarrow(&to_type)?.into_arrow_robj()
}

extendr_module! {
    mod winding;
    fn ga_reverse;
    fn ga_orient;
    fn ga_winding_order;
    fn ga_is_ccw;
    fn ga_is_cw;
}
