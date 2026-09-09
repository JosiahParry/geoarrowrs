use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanBuilder, Date32Builder, Float32Builder, Float64Builder, Int32Builder,
    StringBuilder, TimestampSecondBuilder,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use arrow_extendr::IntoArrowRobj;
use extendr_api::prelude::*;
use geo::{MultiLineString, MultiPoint, MultiPolygon, Point};
use geoarrow::array::{
    MultiLineStringBuilder, MultiPointBuilder, MultiPolygonBuilder, PointBuilder,
};
use geoarrow::datatypes::{
    Crs, Dimension, Metadata, MultiLineStringType, MultiPointType, MultiPolygonType, PointType,
};
use geoarrow::error::GeoArrowError;
use geoarrow_array::GeoArrowArray;
use shapefile::dbase::{FieldType, FieldValue};
use shapefile::record::polygon::PolygonRing;
use shapefile::{
    MultipointM, MultipointZ, NO_DATA, PointM, PointZ, PolygonM, PolygonZ, PolylineM, PolylineZ,
    Shape, ShapeType,
};

/// Swap the extension on a `.shp` path to find a sibling file.
fn sibling(path: &str, ext: &str) -> PathBuf {
    Path::new(path).with_extension(ext)
}

/// An arrow builder for one dbase attribute column, typed from the `.dbf` header.
enum ColBuilder {
    Str(StringBuilder),
    F64(Float64Builder),
    F32(Float32Builder),
    I32(Int32Builder),
    Bool(BooleanBuilder),
    Date(Date32Builder),
    DateTime(TimestampSecondBuilder),
}

impl ColBuilder {
    fn new(field_type: FieldType) -> Self {
        match field_type {
            FieldType::Character | FieldType::Memo => ColBuilder::Str(StringBuilder::new()),
            FieldType::Numeric | FieldType::Double | FieldType::Currency => {
                ColBuilder::F64(Float64Builder::new())
            }
            FieldType::Float => ColBuilder::F32(Float32Builder::new()),
            FieldType::Integer => ColBuilder::I32(Int32Builder::new()),
            FieldType::Logical => ColBuilder::Bool(BooleanBuilder::new()),
            FieldType::Date => ColBuilder::Date(Date32Builder::new()),
            FieldType::DateTime => ColBuilder::DateTime(TimestampSecondBuilder::new()),
        }
    }

    fn data_type(&self) -> DataType {
        match self {
            ColBuilder::Str(_) => DataType::Utf8,
            ColBuilder::F64(_) => DataType::Float64,
            ColBuilder::F32(_) => DataType::Float32,
            ColBuilder::I32(_) => DataType::Int32,
            ColBuilder::Bool(_) => DataType::Boolean,
            ColBuilder::Date(_) => DataType::Date32,
            ColBuilder::DateTime(_) => DataType::Timestamp(TimeUnit::Second, None),
        }
    }

    /// Append one value. A missing field, or a value whose variant does not
    /// match the column, appends a null rather than failing the whole read.
    fn push(&mut self, value: Option<FieldValue>) {
        match self {
            ColBuilder::Str(b) => match value {
                Some(FieldValue::Character(v)) => b.append_option(v),
                Some(FieldValue::Memo(v)) => b.append_value(v),
                _ => b.append_null(),
            },
            ColBuilder::F64(b) => match value {
                Some(FieldValue::Numeric(v)) => b.append_option(v),
                Some(FieldValue::Double(v)) | Some(FieldValue::Currency(v)) => b.append_value(v),
                _ => b.append_null(),
            },
            ColBuilder::F32(b) => match value {
                Some(FieldValue::Float(v)) => b.append_option(v),
                _ => b.append_null(),
            },
            ColBuilder::I32(b) => match value {
                Some(FieldValue::Integer(v)) => b.append_value(v),
                _ => b.append_null(),
            },
            ColBuilder::Bool(b) => match value {
                Some(FieldValue::Logical(v)) => b.append_option(v),
                _ => b.append_null(),
            },
            ColBuilder::Date(b) => match value {
                Some(FieldValue::Date(Some(d))) => b.append_value(d.to_unix_days()),
                _ => b.append_null(),
            },
            ColBuilder::DateTime(b) => match value {
                Some(FieldValue::DateTime(dt)) => b.append_value(dt.to_unix_timestamp()),
                _ => b.append_null(),
            },
        }
    }

    fn finish(self) -> ArrayRef {
        match self {
            ColBuilder::Str(mut b) => Arc::new(b.finish()),
            ColBuilder::F64(mut b) => Arc::new(b.finish()),
            ColBuilder::F32(mut b) => Arc::new(b.finish()),
            ColBuilder::I32(mut b) => Arc::new(b.finish()),
            ColBuilder::Bool(mut b) => Arc::new(b.finish()),
            ColBuilder::Date(mut b) => Arc::new(b.finish()),
            ColBuilder::DateTime(mut b) => Arc::new(b.finish()),
        }
    }
}

/// Whether coordinates carry M. The header's M range is 0 when absent and 0 > NO_DATA, so only the per-coordinate sentinel is reliable.
#[derive(Clone, Copy, PartialEq, Eq)]
enum MPresence {
    None,
    All,
    Mixed,
}

fn bump(m: f64, with: &mut usize, total: &mut usize) {
    *total += 1;
    if m > NO_DATA {
        *with += 1;
    }
}

fn m_presence(shapes: &[Shape]) -> MPresence {
    let mut with = 0usize;
    let mut total = 0usize;
    for shape in shapes {
        match shape {
            Shape::PointM(p) => bump(p.m, &mut with, &mut total),
            Shape::PointZ(p) => bump(p.m, &mut with, &mut total),
            Shape::MultipointM(g) => {
                for p in g.points() {
                    bump(p.m, &mut with, &mut total)
                }
            }
            Shape::MultipointZ(g) => {
                for p in g.points() {
                    bump(p.m, &mut with, &mut total)
                }
            }
            Shape::PolylineM(g) => {
                for p in g.parts().iter().flatten() {
                    bump(p.m, &mut with, &mut total)
                }
            }
            Shape::PolylineZ(g) => {
                for p in g.parts().iter().flatten() {
                    bump(p.m, &mut with, &mut total)
                }
            }
            Shape::PolygonM(g) => {
                for p in g.rings().iter().flat_map(|r| r.points()) {
                    bump(p.m, &mut with, &mut total)
                }
            }
            Shape::PolygonZ(g) => {
                for p in g.rings().iter().flat_map(|r| r.points()) {
                    bump(p.m, &mut with, &mut total)
                }
            }
            _ => {}
        }
    }

    if with == 0 {
        MPresence::None
    } else if with == total {
        MPresence::All
    } else {
        MPresence::Mixed
    }
}

/// Pick one coordinate dimension for the whole file from the shape type and whether M is present.
fn dimension_for(shape_type: ShapeType, has_m: bool) -> Option<Dimension> {
    use ShapeType::*;
    match shape_type {
        Point | Polyline | Polygon | Multipoint => Some(Dimension::XY),
        PointM | PolylineM | PolygonM | MultipointM => {
            Some(if has_m { Dimension::XYM } else { Dimension::XY })
        }
        PointZ | PolylineZ | PolygonZ | MultipointZ => Some(if has_m {
            Dimension::XYZM
        } else {
            Dimension::XYZ
        }),
        NullShape | Multipatch => None,
    }
}

// Forcing `m` to the sentinel makes every coordinate report the narrow dimension; NaN cannot widen, as `nth_or_panic(3)` panics on it.
fn norm_m(mut p: PointM, keep_m: bool) -> PointM {
    if !keep_m {
        p.m = NO_DATA;
    }
    p
}

fn norm_z(mut p: PointZ, keep_m: bool) -> PointZ {
    if !keep_m {
        p.m = NO_DATA;
    }
    p
}

fn norm_ring<P, F: Fn(P) -> P>(ring: PolygonRing<P>, f: &F) -> PolygonRing<P> {
    match ring {
        PolygonRing::Outer(pts) => PolygonRing::Outer(pts.into_iter().map(f).collect()),
        PolygonRing::Inner(pts) => PolygonRing::Inner(pts.into_iter().map(f).collect()),
    }
}

/// Build the geometry column, dispatching on the `.shp` header's shape type.
fn build_geometry(
    shape_type: ShapeType,
    has_m: bool,
    shapes: Vec<Shape>,
    metadata: Arc<Metadata>,
) -> extendr_api::Result<(ArrayRef, Arc<Field>)> {
    let err = |e: GeoArrowError| Error::Other(e.to_string());
    let dim = dimension_for(shape_type, has_m).ok_or_else(|| {
        Error::Other(format!(
            "shape type `{shape_type}` is not supported; Multipatch and NullShape files cannot be read"
        ))
    })?;
    let keep_m = matches!(dim, Dimension::XYM | Dimension::XYZM);
    let wrong = |t: &str| Error::Other(format!("unexpected shape in a {t} file"));

    match shape_type {
        ShapeType::Point | ShapeType::PointM | ShapeType::PointZ => {
            let mut bldr = PointBuilder::new(PointType::new(dim, metadata));
            for shape in shapes {
                // push_point panics on a dimension mismatch and would abort R; this raises instead
                match shape {
                    Shape::Point(p) => bldr.try_push_point(Some(&p)).map_err(err)?,
                    Shape::PointM(p) => {
                        bldr.try_push_point(Some(&norm_m(p, keep_m))).map_err(err)?
                    }
                    Shape::PointZ(p) => {
                        bldr.try_push_point(Some(&norm_z(p, keep_m))).map_err(err)?
                    }
                    Shape::NullShape => bldr.try_push_point(None::<&Point<f64>>).map_err(err)?,
                    _ => return Err(wrong("point")),
                }
            }
            let arr = bldr.finish();
            Ok((
                arr.to_array_ref(),
                arr.data_type().to_field("geometry", true).into(),
            ))
        }
        ShapeType::Multipoint | ShapeType::MultipointM | ShapeType::MultipointZ => {
            let mut bldr = MultiPointBuilder::new(MultiPointType::new(dim, metadata));
            for shape in shapes {
                match shape {
                    Shape::Multipoint(g) => bldr.push_multi_point(Some(&g)).map_err(err)?,
                    Shape::MultipointM(g) => {
                        let g = MultipointM::new(
                            g.into_inner()
                                .into_iter()
                                .map(|p| norm_m(p, keep_m))
                                .collect(),
                        );
                        bldr.push_multi_point(Some(&g)).map_err(err)?
                    }
                    Shape::MultipointZ(g) => {
                        let g = MultipointZ::new(
                            g.into_inner()
                                .into_iter()
                                .map(|p| norm_z(p, keep_m))
                                .collect(),
                        );
                        bldr.push_multi_point(Some(&g)).map_err(err)?
                    }
                    Shape::NullShape => bldr
                        .push_multi_point(None::<&MultiPoint<f64>>)
                        .map_err(err)?,
                    _ => return Err(wrong("multipoint")),
                }
            }
            let arr = bldr.finish();
            Ok((
                arr.to_array_ref(),
                arr.data_type().to_field("geometry", true).into(),
            ))
        }
        ShapeType::Polyline | ShapeType::PolylineM | ShapeType::PolylineZ => {
            let mut bldr = MultiLineStringBuilder::new(MultiLineStringType::new(dim, metadata));
            for shape in shapes {
                match shape {
                    Shape::Polyline(g) => bldr.push_multi_line_string(Some(&g)).map_err(err)?,
                    Shape::PolylineM(g) => {
                        let parts = g
                            .into_inner()
                            .into_iter()
                            .map(|part| part.into_iter().map(|p| norm_m(p, keep_m)).collect())
                            .collect();
                        bldr.push_multi_line_string(Some(&PolylineM::with_parts(parts)))
                            .map_err(err)?
                    }
                    Shape::PolylineZ(g) => {
                        let parts = g
                            .into_inner()
                            .into_iter()
                            .map(|part| part.into_iter().map(|p| norm_z(p, keep_m)).collect())
                            .collect();
                        bldr.push_multi_line_string(Some(&PolylineZ::with_parts(parts)))
                            .map_err(err)?
                    }
                    Shape::NullShape => bldr
                        .push_multi_line_string(None::<&MultiLineString<f64>>)
                        .map_err(err)?,
                    _ => return Err(wrong("polyline")),
                }
            }
            let arr = bldr.finish();
            Ok((
                arr.to_array_ref(),
                arr.data_type().to_field("geometry", true).into(),
            ))
        }
        ShapeType::Polygon | ShapeType::PolygonM | ShapeType::PolygonZ => {
            let mut bldr = MultiPolygonBuilder::new(MultiPolygonType::new(dim, metadata));
            for shape in shapes {
                // rings are unordered; try_into_geo_traits resolves outer/inner into a multipolygon
                match shape {
                    Shape::Polygon(g) => {
                        let mp = g
                            .try_into_geo_traits()
                            .map_err(|e| Error::Other(e.to_string()))?;
                        bldr.push_multi_polygon(Some(&mp)).map_err(err)?
                    }
                    Shape::PolygonM(g) => {
                        let f = |p| norm_m(p, keep_m);
                        let rings = g
                            .into_inner()
                            .into_iter()
                            .map(|r| norm_ring(r, &f))
                            .collect();
                        let mp = PolygonM::with_rings(rings)
                            .try_into_geo_traits()
                            .map_err(|e| Error::Other(e.to_string()))?;
                        bldr.push_multi_polygon(Some(&mp)).map_err(err)?
                    }
                    Shape::PolygonZ(g) => {
                        let f = |p| norm_z(p, keep_m);
                        let rings = g
                            .into_inner()
                            .into_iter()
                            .map(|r| norm_ring(r, &f))
                            .collect();
                        let mp = PolygonZ::with_rings(rings)
                            .try_into_geo_traits()
                            .map_err(|e| Error::Other(e.to_string()))?;
                        bldr.push_multi_polygon(Some(&mp)).map_err(err)?
                    }
                    Shape::NullShape => bldr
                        .push_multi_polygon(None::<&MultiPolygon<f64>>)
                        .map_err(err)?,
                    _ => return Err(wrong("polygon")),
                }
            }
            let arr = bldr.finish();
            Ok((
                arr.to_array_ref(),
                arr.data_type().to_field("geometry", true).into(),
            ))
        }
        other => Err(Error::Other(format!(
            "shape type `{other}` is not supported yet"
        ))),
    }
}

/// Read an ESRI Shapefile into a record batch stream
///
/// Reads the `.shp` geometries together with the `.dbf` attributes. The CRS is
/// taken from the sibling `.prj` file when one is present.
///
/// @details
/// Polylines are read as multilinestrings and polygons as multipolygons,
/// because a shapefile record of either type may contain multiple parts.
///
/// The coordinate dimension is chosen once per file, from the shape type and
/// from whether the coordinates actually carry measures. A `PolygonZ` file with
/// no measures is therefore read as XYZ, rather than as XYZM full of nulls.
///
/// | shape type | measures present | dimension |
/// | --- | --- | --- |
/// | `Point`, `Polyline`, `Polygon`, `Multipoint` | not applicable | XY |
/// | `PointM`, `PolylineM`, `PolygonM`, `MultipointM` | no | XY |
/// | `PointM`, `PolylineM`, `PolygonM`, `MultipointM` | yes | XYM |
/// | `PointZ`, `PolylineZ`, `PolygonZ`, `MultipointZ` | no | XYZ |
/// | `PointZ`, `PolylineZ`, `PolygonZ`, `MultipointZ` | yes | XYZM |
///
/// If only some coordinates carry a measure, M is dropped and a message is
/// emitted, because an Arrow column cannot mix dimensions.
///
/// `Multipatch` files are not supported.
///
/// @param path path to a `.shp` file. The sibling `.dbf` and `.prj` files are
///   resolved from the same base name.
/// @returns a `nanoarrow_array_stream` of a single record batch, holding the
///   `.dbf` attribute columns followed by a `geometry` column.
/// @examplesIf requireNamespace("sf", quietly = TRUE) && requireNamespace("geoarrow", quietly = TRUE)
/// path <- system.file("shape/nc.shp", package = "sf")
/// df <- as.data.frame(read_shapefile(path))
///
/// # the .dbf attribute columns, followed by `geometry`
/// dim(df)
/// head(df[, c("NAME", "BIR74", "geometry")], 3)
/// @export
/// @family io
#[extendr]
fn read_shapefile(path: &str) -> extendr_api::Result<Robj> {
    let mut reader = shapefile::Reader::from_path(path).map_err(|e| Error::Other(e.to_string()))?;
    let shape_type = reader.header().shape_type;

    // shapefile::Reader's dbase reader is private, so reopen the .dbf for field order; dbase::Record is a HashMap
    let dbf_path = sibling(path, "dbf");
    let dbf = shapefile::dbase::Reader::from_path(&dbf_path)
        .map_err(|e| Error::Other(format!("failed to open {}: {e}", dbf_path.display())))?;
    let fields: Vec<(String, FieldType)> = dbf
        .fields()
        .iter()
        .map(|f| (f.name().to_string(), f.field_type()))
        .collect();

    // the .prj holds WKT1, which is not a CRS encoding geoarrow names explicitly
    let metadata = match std::fs::read_to_string(sibling(path, "prj")) {
        Ok(prj) => Arc::new(Metadata::new(
            Crs::from_unknown_crs_type(prj.trim().to_string()),
            None,
        )),
        Err(_) => Arc::new(Metadata::default()),
    };

    let mut shapes = Vec::new();
    let mut builders: Vec<ColBuilder> = fields.iter().map(|(_, ft)| ColBuilder::new(*ft)).collect();

    for item in reader.iter_shapes_and_records() {
        let (shape, mut record) = item.map_err(|e| Error::Other(e.to_string()))?;
        shapes.push(shape);
        for ((name, _), bldr) in fields.iter().zip(builders.iter_mut()) {
            bldr.push(record.remove(name));
        }
    }

    let presence = m_presence(&shapes);
    if presence == MPresence::Mixed {
        // FIXME: should raise a catchable R warning, not just print
        rprintln!(
            "Some coordinates carry an M value and others do not; M has been dropped, \
             because an Arrow column cannot mix dimensions."
        );
    }
    let has_m = presence == MPresence::All;

    let (geometry, geometry_field) = build_geometry(shape_type, has_m, shapes, metadata)?;

    let mut arrow_fields: Vec<Arc<Field>> = Vec::with_capacity(fields.len() + 1);
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(fields.len() + 1);
    for ((name, _), bldr) in fields.into_iter().zip(builders) {
        arrow_fields.push(Arc::new(Field::new(name, bldr.data_type(), true)));
        columns.push(bldr.finish());
    }
    arrow_fields.push(geometry_field);
    columns.push(geometry);

    let schema = Arc::new(Schema::new(arrow_fields));
    let batch = RecordBatch::try_new(schema, columns).map_err(|e| Error::Other(e.to_string()))?;

    vec![batch].into_arrow_robj()
}

extendr_module! {
    mod shapefile;
    fn read_shapefile;
}
