use std::str::FromStr;
use std::sync::Arc;

use arrow::array::{Array, BooleanBuilder, StringBuilder};
use arrow_extendr::IntoArrowRobj;
use extendr_api::prelude::*;
use geo::Geometry;
use geoarrow::datatypes::{
    Dimension, GeoArrowType, Metadata, MultiLineStringType, MultiPointType, MultiPolygonType,
};
use geoarrow_array::GeoArrowArray;
use geoarrow_cast::downcast::NativeType;

use crate::{as_geo_geometries, as_geometry_chunks};

/// A `NativeType` read from and written as its simple features name, such as `"MULTIPOLYGON"`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct TypeName(pub(crate) NativeType);

impl TypeName {
    /// The types `FromStr` accepts, in the order sf lists them; a rect is read as a polygon.
    const ALL: [NativeType; 7] = [
        NativeType::Point,
        NativeType::LineString,
        NativeType::Polygon,
        NativeType::MultiPoint,
        NativeType::MultiLineString,
        NativeType::MultiPolygon,
        NativeType::GeometryCollection,
    ];

    /// The multi type that gathers parts of this single type, or None for any other type.
    pub(crate) fn multi_type(&self, metadata: Arc<Metadata>) -> Option<GeoArrowType> {
        match self.0 {
            NativeType::Point => Some(GeoArrowType::MultiPoint(MultiPointType::new(
                Dimension::XY,
                metadata,
            ))),
            NativeType::LineString => Some(GeoArrowType::MultiLineString(
                MultiLineStringType::new(Dimension::XY, metadata),
            )),
            NativeType::Polygon => Some(GeoArrowType::MultiPolygon(MultiPolygonType::new(
                Dimension::XY,
                metadata,
            ))),
            _ => None,
        }
    }
}

impl AsRef<str> for TypeName {
    fn as_ref(&self) -> &str {
        match self.0 {
            NativeType::Point => "POINT",
            NativeType::LineString => "LINESTRING",
            NativeType::Polygon | NativeType::Rect => "POLYGON",
            NativeType::MultiPoint => "MULTIPOINT",
            NativeType::MultiLineString => "MULTILINESTRING",
            NativeType::MultiPolygon => "MULTIPOLYGON",
            NativeType::GeometryCollection => "GEOMETRYCOLLECTION",
        }
    }
}

impl FromStr for TypeName {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        Self::ALL
            .into_iter()
            .map(TypeName)
            .find(|t| t.as_ref() == s)
            .ok_or_else(|| {
                let names = Self::ALL
                    .map(|t| format!("\"{}\"", TypeName(t).as_ref()))
                    .join(", ");
                format!("must be one of {names}, got \"{s}\"")
            })
    }
}

/// geo's line is a linestring, and its rect and triangle are polygons, as they encode to.
impl From<&Geometry<f64>> for TypeName {
    fn from(g: &Geometry<f64>) -> Self {
        TypeName(match g {
            Geometry::Point(_) => NativeType::Point,
            Geometry::Line(_) | Geometry::LineString(_) => NativeType::LineString,
            Geometry::Polygon(_) | Geometry::Rect(_) | Geometry::Triangle(_) => NativeType::Polygon,
            Geometry::MultiPoint(_) => NativeType::MultiPoint,
            Geometry::MultiLineString(_) => NativeType::MultiLineString,
            Geometry::MultiPolygon(_) => NativeType::MultiPolygon,
            Geometry::GeometryCollection(_) => NativeType::GeometryCollection,
        })
    }
}

/// Determine the type of each geometry
///
/// Returns the simple features type name of each geometry, such as `"POINT"`
/// or `"MULTIPOLYGON"`. `ga_geometry_type()` uses the same names as
/// `sf::st_geometry_type()`.
///
/// @details
/// `ga_geometry_type()` returns `NA` for a null geometry. It does the same for
/// an empty point, which has no coordinates to read a geometry from.
///
/// @param geometry a GeoArrow geometry array
/// @returns a string array of the same length as `geometry`
/// @export
/// @family geometry type
/// @examplesIf requireNamespace("sf", quietly = TRUE) && requireNamespace("geoarrow", quietly = TRUE)
/// g <- geoarrow::as_geoarrow_array(sf::st_sfc(
///   sf::st_point(c(0, 0)),
///   sf::st_linestring(rbind(c(0, 0), c(1, 1)))
/// ))
///
/// as.vector(ga_geometry_type(g))
#[extendr]
fn ga_geometry_type(geometry: Robj) -> extendr_api::Result<Robj> {
    let chunks = as_geometry_chunks(geometry)?;
    let n = chunks.iter().map(|c| c.len()).sum();
    let mut bldr = StringBuilder::with_capacity(n, n * 8);

    for chunk in &chunks {
        for geom in as_geo_geometries(chunk.as_ref())? {
            match geom {
                Some(g) => bldr.append_value(TypeName::from(&g)),
                None => bldr.append_null(),
            }
        }
    }

    bldr.finish().into_data().into_arrow_robj()
}

/// Test whether geometries are of a given type
///
/// `TRUE` when the geometry's type is one of `types`. Type names are those
/// `ga_geometry_type()` returns.
///
/// @details
/// `ga_is()` returns `NA` for a null geometry. It errors on an unknown type
/// name rather than returning `FALSE`, so it never silently passes a
/// misspelling.
///
/// @param geometry a GeoArrow geometry array
/// @param types a character vector of simple features type names, such as `"POLYGON"`
/// @returns a boolean array of the same length as `geometry`
/// @export
/// @family geometry type
/// @examplesIf requireNamespace("sf", quietly = TRUE) && requireNamespace("geoarrow", quietly = TRUE)
/// g <- geoarrow::as_geoarrow_array(sf::st_sfc(
///   sf::st_point(c(0, 0)),
///   sf::st_linestring(rbind(c(0, 0), c(1, 1)))
/// ))
///
/// as.vector(ga_is(g, "POINT"))
/// as.vector(ga_is(g, c("POINT", "LINESTRING")))
#[extendr]
fn ga_is(geometry: Robj, types: Robj) -> extendr_api::Result<Robj> {
    let Some(names) = types.as_str_vector() else {
        return Err(Error::Other(
            "`types` must be a character vector".to_string(),
        ));
    };
    let wanted = names
        .iter()
        .map(|t| t.parse::<TypeName>())
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| Error::Other(format!("`types` {e}")))?;

    let chunks = as_geometry_chunks(geometry)?;
    let n = chunks.iter().map(|c| c.len()).sum();
    let mut bldr = BooleanBuilder::with_capacity(n);

    for chunk in &chunks {
        for geom in as_geo_geometries(chunk.as_ref())? {
            match geom {
                Some(g) => bldr.append_value(wanted.contains(&TypeName::from(&g))),
                None => bldr.append_null(),
            }
        }
    }

    bldr.finish().into_data().into_arrow_robj()
}

extendr_module! {
    mod geometry_type;
    fn ga_geometry_type;
    fn ga_is;
}
