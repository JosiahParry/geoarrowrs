use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanBuilder, Float64Builder, Int64Builder, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow_extendr::IntoArrowRobj;
use extendr_api::prelude::*;
use geoarrow::array::{
    GeometryBuilder, LineStringBuilder, MultiLineStringBuilder, MultiPointBuilder,
    MultiPolygonBuilder, PointBuilder, PolygonBuilder,
};
use geoarrow::datatypes::{
    Crs, Dimension, GeometryType, LineStringType, Metadata, MultiLineStringType, MultiPointType,
    MultiPolygonType, PointType, PolygonType,
};
use geoarrow::error::GeoArrowError;
use geoarrow_array::GeoArrowArray;

/// The widest arrow type a GeoJSON property needs, widened across all features.
#[derive(Clone, Copy, PartialEq, Eq)]
enum PropType {
    Null,
    Bool,
    Int,
    Float,
    Str,
}

/// JSON is untyped per feature, so a key seen as both int and float becomes float, and any pairing that cannot unify falls back to string.
fn widen(a: PropType, b: PropType) -> PropType {
    use PropType::*;
    match (a, b) {
        (Null, x) | (x, Null) => x,
        (x, y) if x == y => x,
        (Int, Float) | (Float, Int) => Float,
        _ => Str,
    }
}

fn type_of(value: &::geojson::JsonValue) -> PropType {
    match value {
        ::geojson::JsonValue::Null => PropType::Null,
        ::geojson::JsonValue::Bool(_) => PropType::Bool,
        ::geojson::JsonValue::Number(n) => {
            if n.is_i64() || n.is_u64() {
                PropType::Int
            } else {
                PropType::Float
            }
        }
        ::geojson::JsonValue::String(_) => PropType::Str,
        // arrays and objects have no arrow analogue, so keep the raw JSON text
        _ => PropType::Str,
    }
}

/// An arrow builder for one GeoJSON property column.
enum ColBuilder {
    Bool(BooleanBuilder),
    Int(Int64Builder),
    Float(Float64Builder),
    Str(StringBuilder),
}

impl ColBuilder {
    fn new(prop_type: PropType) -> Self {
        match prop_type {
            PropType::Bool => ColBuilder::Bool(BooleanBuilder::new()),
            PropType::Int => ColBuilder::Int(Int64Builder::new()),
            PropType::Float => ColBuilder::Float(Float64Builder::new()),
            // a column that was null in every feature becomes an all-null string column
            PropType::Str | PropType::Null => ColBuilder::Str(StringBuilder::new()),
        }
    }

    fn data_type(&self) -> DataType {
        match self {
            ColBuilder::Bool(_) => DataType::Boolean,
            ColBuilder::Int(_) => DataType::Int64,
            ColBuilder::Float(_) => DataType::Float64,
            ColBuilder::Str(_) => DataType::Utf8,
        }
    }

    fn push(&mut self, value: Option<&::geojson::JsonValue>) {
        use ::geojson::JsonValue as V;
        match self {
            ColBuilder::Bool(b) => match value {
                Some(V::Bool(v)) => b.append_value(*v),
                _ => b.append_null(),
            },
            ColBuilder::Int(b) => match value.and_then(|v| v.as_i64()) {
                Some(v) => b.append_value(v),
                None => b.append_null(),
            },
            ColBuilder::Float(b) => match value.and_then(|v| v.as_f64()) {
                Some(v) => b.append_value(v),
                None => b.append_null(),
            },
            ColBuilder::Str(b) => match value {
                None | Some(V::Null) => b.append_null(),
                Some(V::String(v)) => b.append_value(v),
                // numbers, bools, arrays and objects keep their JSON text
                Some(other) => b.append_value(other.to_string()),
            },
        }
    }

    fn finish(self) -> ArrayRef {
        match self {
            ColBuilder::Bool(mut b) => Arc::new(b.finish()),
            ColBuilder::Int(mut b) => Arc::new(b.finish()),
            ColBuilder::Float(mut b) => Arc::new(b.finish()),
            ColBuilder::Str(mut b) => Arc::new(b.finish()),
        }
    }
}

/// The narrowest geoarrow type that holds every geometry in the collection.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Target {
    Point,
    LineString,
    Polygon,
    MultiPoint,
    MultiLineString,
    MultiPolygon,
    Mixed,
}

/// A mixed `geoarrow.geometry` column is a dense union that geoarrow's R bindings cannot read, so narrow to a concrete type whenever the collection allows it.
fn target_for(features: &[::geojson::Feature]) -> Target {
    use ::geojson::Value::*;
    let mut point = false;
    let mut line = false;
    let mut poly = false;
    let mut multi_point = false;
    let mut multi_line = false;
    let mut multi_poly = false;
    let mut other = false;

    for f in features {
        match f.geometry.as_ref().map(|g| &g.value) {
            Some(Point(_)) => point = true,
            Some(LineString(_)) => line = true,
            Some(Polygon(_)) => poly = true,
            Some(MultiPoint(_)) => multi_point = true,
            Some(MultiLineString(_)) => multi_line = true,
            Some(MultiPolygon(_)) => multi_poly = true,
            Some(GeometryCollection(_)) => other = true,
            None => {}
        }
    }

    if other {
        return Target::Mixed;
    }
    let families = [point || multi_point, line || multi_line, poly || multi_poly];
    if families.iter().filter(|x| **x).count() > 1 {
        return Target::Mixed;
    }

    match (point, multi_point, line, multi_line, poly, multi_poly) {
        (true, false, ..) => Target::Point,
        (_, true, ..) => Target::MultiPoint,
        (_, _, true, false, ..) => Target::LineString,
        (_, _, _, true, ..) => Target::MultiLineString,
        (_, _, _, _, true, false) => Target::Polygon,
        (_, _, _, _, _, true) => Target::MultiPolygon,
        _ => Target::Mixed,
    }
}

fn to_geo(g: &::geojson::Geometry) -> extendr_api::Result<geo::Geometry<f64>> {
    (&g.value)
        .try_into()
        .map_err(|e: ::geojson::Error| Error::Other(e.to_string()))
}

macro_rules! build_narrowed {
    ($features:expr, $bldr:expr, $push:ident, $none:ty, $extract:expr) => {{
        let mut bldr = $bldr;
        for f in $features {
            match f.geometry.as_ref() {
                Some(g) => {
                    let geom = to_geo(g)?;
                    let narrowed = $extract(geom).ok_or_else(|| {
                        Error::Other("geometry did not match the collection's type".to_string())
                    })?;
                    bldr.$push(Some(&narrowed))
                        .map_err(|e: GeoArrowError| Error::Other(e.to_string()))?;
                }
                None => bldr
                    .$push(None::<$none>)
                    .map_err(|e: GeoArrowError| Error::Other(e.to_string()))?,
            }
        }
        let arr = bldr.finish();
        Ok((
            arr.to_array_ref(),
            Arc::new(arr.data_type().to_field("geometry", true)),
        ))
    }};
}

fn build_geometry(
    features: &[::geojson::Feature],
    metadata: Arc<Metadata>,
) -> extendr_api::Result<(ArrayRef, Arc<Field>)> {
    use geo::Geometry as G;

    match target_for(features) {
        Target::Point => build_narrowed!(
            features,
            PointBuilder::new(PointType::new(Dimension::XY, metadata)),
            try_push_point,
            &geo::Point<f64>,
            |g| match g {
                G::Point(p) => Some(p),
                _ => None,
            }
        ),
        Target::LineString => build_narrowed!(
            features,
            LineStringBuilder::new(LineStringType::new(Dimension::XY, metadata)),
            push_line_string,
            &geo::LineString<f64>,
            |g| match g {
                G::LineString(l) => Some(l),
                _ => None,
            }
        ),
        Target::Polygon => build_narrowed!(
            features,
            PolygonBuilder::new(PolygonType::new(Dimension::XY, metadata)),
            push_polygon,
            &geo::Polygon<f64>,
            |g| match g {
                G::Polygon(p) => Some(p),
                _ => None,
            }
        ),
        Target::MultiPoint => build_narrowed!(
            features,
            MultiPointBuilder::new(MultiPointType::new(Dimension::XY, metadata)),
            push_multi_point,
            &geo::MultiPoint<f64>,
            |g| match g {
                G::Point(p) => Some(geo::MultiPoint(vec![p])),
                G::MultiPoint(mp) => Some(mp),
                _ => None,
            }
        ),
        Target::MultiLineString => build_narrowed!(
            features,
            MultiLineStringBuilder::new(MultiLineStringType::new(Dimension::XY, metadata)),
            push_multi_line_string,
            &geo::MultiLineString<f64>,
            |g| match g {
                G::LineString(l) => Some(geo::MultiLineString(vec![l])),
                G::MultiLineString(ml) => Some(ml),
                _ => None,
            }
        ),
        Target::MultiPolygon => build_narrowed!(
            features,
            MultiPolygonBuilder::new(MultiPolygonType::new(Dimension::XY, metadata)),
            push_multi_polygon,
            &geo::MultiPolygon<f64>,
            |g| match g {
                G::Polygon(p) => Some(geo::MultiPolygon(vec![p])),
                G::MultiPolygon(mp) => Some(mp),
                _ => None,
            }
        ),
        Target::Mixed => build_narrowed!(
            features,
            GeometryBuilder::new(GeometryType::new(metadata)),
            push_geometry,
            &geo::Geometry<f64>,
            Some
        ),
    }
}

/// Read a GeoJSON FeatureCollection
///
/// Reads the geometries together with the feature properties. Properties are
/// untyped in GeoJSON, so each column's arrow type is inferred by scanning
/// every feature.
///
/// @details
/// The geometry column is narrowed to the most specific type that holds every
/// feature, promoting a singular type to its multi form where the two are
/// mixed. A feature with a `null` geometry becomes a null element and does not
/// affect the choice:
///
/// | geometry types in the file | column type |
/// | --- | --- |
/// | all points | `geoarrow.point` |
/// | points and multipoints | `geoarrow.multipoint` |
/// | all polygons | `geoarrow.polygon` |
/// | polygons and multipolygons | `geoarrow.multipolygon` |
/// | more than one family, or any collection | `geoarrow.geometry` |
///
/// Narrowing matters in practice: `geoarrow.geometry` is a dense union that
/// geoarrow's R bindings cannot yet convert, so a genuinely mixed collection
/// reads back as its raw storage rather than as geometries.
///
/// Property columns appear in the order the keys are first seen. A key missing
/// from a given feature is null for that row. Types are widened across
/// features:
///
/// | values seen for a key | column type |
/// | --- | --- |
/// | booleans | `Boolean` |
/// | integers | `Int64` |
/// | integers and reals | `Float64` |
/// | strings, or any mix that cannot unify | `Utf8` |
/// | only `null` | `Utf8`, all null |
///
/// Nested arrays and objects have no arrow analogue and are kept as their raw
/// JSON text.
///
/// Per RFC 7946 the coordinate reference system is always `OGC:CRS84`, so that
/// is set on the geometry column without reading anything from the file.
///
/// @param path path to a `.geojson` file holding a `FeatureCollection`.
/// @returns a `nanoarrow_array_stream` of a single record batch, holding the
///   property columns followed by a `geometry` column.
/// @examplesIf requireNamespace("sf", quietly = TRUE) && requireNamespace("geoarrow", quietly = TRUE)
/// path <- tempfile(fileext = ".geojson")
/// fp <- system.file("shape/nc.shp", package = "sf")
/// sf::st_write(
///   sf::st_read(fp, quiet = TRUE),
///   path,
///   quiet = TRUE
/// )
///
/// # read into a nanoarrow array stream
/// res <- read_geojson(path)
/// res
///
/// # convert to a df
/// df <- as.data.frame(res)
/// head(df)
/// @export
/// @family io
#[extendr]
fn read_geojson(path: &str) -> extendr_api::Result<Robj> {
    let file = File::open(path).map_err(|e| Error::Other(format!("failed to open {path}: {e}")))?;
    let reader = ::geojson::FeatureReader::from_reader(BufReader::new(file));

    let features = reader
        .features()
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| Error::Other(e.to_string()))?;

    // first-seen key order, since serde_json's map does not preserve the document order
    let mut names: Vec<String> = Vec::new();
    let mut types: Vec<PropType> = Vec::new();
    for feature in &features {
        if let Some(props) = &feature.properties {
            for (key, value) in props {
                match names.iter().position(|n| n == key) {
                    Some(i) => types[i] = widen(types[i], type_of(value)),
                    None => {
                        names.push(key.clone());
                        types.push(type_of(value));
                    }
                }
            }
        }
    }

    let metadata = Arc::new(Metadata::new(
        Crs::from_authority_code("OGC:CRS84".to_string()),
        None,
    ));

    let mut builders: Vec<ColBuilder> = types.iter().map(|t| ColBuilder::new(*t)).collect();
    for feature in &features {
        for (name, bldr) in names.iter().zip(builders.iter_mut()) {
            bldr.push(feature.properties.as_ref().and_then(|p| p.get(name)));
        }
    }

    let (geometry, geometry_field) = build_geometry(&features, metadata)?;

    let mut fields: Vec<Arc<Field>> = Vec::with_capacity(names.len() + 1);
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(names.len() + 1);
    for (name, bldr) in names.into_iter().zip(builders) {
        fields.push(Arc::new(Field::new(name, bldr.data_type(), true)));
        columns.push(bldr.finish());
    }

    fields.push(geometry_field);
    columns.push(geometry);

    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
        .map_err(|e| Error::Other(e.to_string()))?;

    vec![batch].into_arrow_robj()
}

extendr_module! {
    mod geojson;
    fn read_geojson;
}
