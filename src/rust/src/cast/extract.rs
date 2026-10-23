use arrow_extendr::IntoArrowRobj;
use extendr_api::prelude::*;
use geo::{Geometry, LineString, MultiLineString, MultiPoint, MultiPolygon, Point, Polygon};
use geoarrow::datatypes::{
    Dimension, GeoArrowType, MultiLineStringType, MultiPointType, MultiPolygonType,
};
use geoarrow_array::GeoArrowArray;
use geoarrow_cast::downcast::NativeType;

use super::CastGeoArrow;
use crate::geometry_type::TypeName;
use crate::threads::Threads;
use crate::{as_geo_geometries_par, as_geometry_chunks};

/// The single parts of a geometry, sorted by family.
#[derive(Default)]
struct Parts {
    points: Vec<Point<f64>>,
    lines: Vec<LineString<f64>>,
    polygons: Vec<Polygon<f64>>,
}

/// Flatten each geometry into its single parts, descending into nested collections.
impl Extend<Geometry<f64>> for Parts {
    fn extend<I: IntoIterator<Item = Geometry<f64>>>(&mut self, iter: I) {
        for g in iter {
            match g {
                Geometry::Point(p) => self.points.push(p),
                Geometry::MultiPoint(m) => self.points.extend(m.0),
                Geometry::Line(l) => self.lines.push(LineString(vec![l.start, l.end])),
                Geometry::LineString(ls) => self.lines.push(ls),
                Geometry::MultiLineString(m) => self.lines.extend(m.0),
                Geometry::Polygon(p) => self.polygons.push(p),
                Geometry::MultiPolygon(m) => self.polygons.extend(m.0),
                Geometry::Rect(r) => self.polygons.push(r.to_polygon()),
                Geometry::Triangle(t) => self.polygons.push(t.to_polygon()),
                Geometry::GeometryCollection(gc) => self.extend(gc.0),
            }
        }
    }
}

impl Parts {
    /// The parts of one single type as the multi geometry of that type, or None when there are none.
    fn into_multi(self, kind: NativeType) -> Option<Geometry<f64>> {
        match kind {
            NativeType::Point if !self.points.is_empty() => {
                Some(Geometry::MultiPoint(MultiPoint(self.points)))
            }
            NativeType::LineString if !self.lines.is_empty() => {
                Some(Geometry::MultiLineString(MultiLineString(self.lines)))
            }
            NativeType::Polygon if !self.polygons.is_empty() => {
                Some(Geometry::MultiPolygon(MultiPolygon(self.polygons)))
            }
            _ => None,
        }
    }
}

/// Extract parts of one type from geometry collections
///
/// Gathers the points, lines, or polygons of each geometry into a single
/// multipoint, multilinestring, or multipolygon. `ga_collection_extract()`
/// flattens nested collections and multi part members in order.
///
/// @details
/// Unlike `sf::st_collection_extract()`, `ga_collection_extract()` keeps one
/// row per input row. It returns a null for a geometry with no parts of
/// `geometry_type`, such as a linestring when you ask for polygons, and for a
/// null geometry.
///
/// @param geometry a GeoArrow geometry array
/// @param geometry_type one of `"POLYGON"`, `"LINESTRING"`, or `"POINT"`
/// @returns a GeoArrow multipolygon, multilinestring, or multipoint array of
///   the same length as `geometry`
/// @export
/// @family cast
/// @examplesIf requireNamespace("sf", quietly = TRUE) && requireNamespace("geoarrow", quietly = TRUE)
/// gc <- sf::st_geometrycollection(list(
///   sf::st_point(c(5, 5)),
///   sf::st_polygon(list(rbind(c(0, 0), c(1, 0), c(1, 1), c(0, 0))))
/// ))
/// g <- geoarrow::as_geoarrow_array(sf::st_sfc(gc, sf::st_point(c(0, 0))))
///
/// # the point has no polygons, so its row is null
/// as.vector(ga_geometry_type(ga_collection_extract(g, "POLYGON")))
#[extendr]
fn ga_collection_extract(
    geometry: Robj,
    #[extendr(default = "\"POLYGON\"")] geometry_type: &str,
) -> extendr_api::Result<Robj> {
    let kind = geometry_type
        .parse::<TypeName>()
        .map_err(|e| Error::Other(format!("`geometry_type` {e}")))?;
    let chunks = as_geometry_chunks(geometry)?;
    let metadata = chunks[0].data_type().metadata().clone();
    let to_type = match kind.0 {
        NativeType::Point => GeoArrowType::MultiPoint(MultiPointType::new(Dimension::XY, metadata)),
        NativeType::LineString => {
            GeoArrowType::MultiLineString(MultiLineStringType::new(Dimension::XY, metadata))
        }
        NativeType::Polygon => {
            GeoArrowType::MultiPolygon(MultiPolygonType::new(Dimension::XY, metadata))
        }
        _ => {
            return Err(Error::Other(format!(
                "`geometry_type` must be one of \"POLYGON\", \"LINESTRING\", or \"POINT\", got \"{}\"",
                kind.as_ref()
            )));
        }
    };

    let geoms = as_geo_geometries_par(Threads::get(), &chunks)?
        .into_iter()
        .map(|g| {
            g.and_then(|g| {
                let mut parts = Parts::default();
                parts.extend([g]);
                parts.into_multi(kind.0)
            })
        })
        .collect::<Vec<_>>();

    geoms.cast_geoarrow(&to_type)?.into_arrow_robj()
}

extendr_module! {
    mod extract;
    fn ga_collection_extract;
}
