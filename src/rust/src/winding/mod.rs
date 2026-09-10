use arrow::array::{Array, BooleanBuilder, StringBuilder};
use arrow_extendr::IntoArrowRobj;
use extendr_api::prelude::*;
use geo::algorithm::orient::{Direction, Orient};
use geo::algorithm::winding_order::WindingOrder;
use geo::{Geometry, MultiPolygon, Polygon, Winding};
use geo_traits::to_geo::{ToGeoMultiPolygon, ToGeoPolygon};
use geoarrow::array::{MultiPolygonBuilder, PolygonBuilder};
use geoarrow::datatypes::{Dimension, MultiPolygonType, PolygonType};
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor};

use crate::{as_geo_geometries, as_geometry_chunks, as_multipolygon_chunks, as_polygon_chunks};

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
/// consistent direction. The geometry type of the output matches the input.
///
/// @details
/// `"default"` gives a counter-clockwise exterior ring and clockwise interior
/// rings, which is the winding the OGC simple features and GeoJSON
/// specifications call for. `"reversed"` gives the opposite. `"ccw"` and
/// `"cw"` are accepted as aliases and refer to the exterior ring.
///
/// Rewinding does not change which points a polygon covers, only the order
/// its coordinates are stored in. A null geometry stays null.
///
/// @param geometry a GeoArrow polygon or multipolygon array
/// @param direction one of `"default"`, `"reversed"`, `"ccw"`, or `"cw"`
/// @returns a GeoArrow array of the same geometry type as `geometry`
/// @export
/// @family winding
/// @references [Orient](https://docs.rs/geo/latest/geo/algorithm/orient/trait.Orient.html)
/// @examplesIf requireNamespace("sf", quietly = TRUE) && requireNamespace("geoarrow", quietly = TRUE)
/// # a clockwise square
/// p <- sf::st_polygon(list(matrix(
///   c(0, 0, 0, 1, 1, 1, 1, 0, 0, 0), ncol = 2, byrow = TRUE
/// )))
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
/// Returns `"clockwise"` or `"counterclockwise"` for each geometry. A polygon
/// reports the winding of its exterior ring.
///
/// @details
/// A multipolygon reports a winding only when every part's exterior ring
/// agrees, and a multilinestring only when every part agrees; a geometry whose
/// parts disagree is `NA`, since it has no single winding. Any other geometry
/// type, a null geometry, or a ring with fewer than three distinct points is
/// also `NA`.
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
/// `TRUE` for clockwise ones. A polygon is tested on its exterior ring.
///
/// @details
/// Any geometry with no winding order, such as a point or a null geometry,
/// becomes `NA` rather than `FALSE`, so the two functions are not simply
/// negations of one another.
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

extendr_module! {
    mod winding;
    fn ga_orient;
    fn ga_winding_order;
    fn ga_is_ccw;
    fn ga_is_cw;
}
