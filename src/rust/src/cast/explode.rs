use std::sync::Arc;

use arrow::array::NullBufferBuilder;
use arrow::array::{Array, ArrayRef, ListArray};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field};
use arrow_extendr::{FromArrowRobj, IntoArrowRobj};
use extendr_api::prelude::*;
use geo::Geometry;
use geoarrow::array::{LineStringBuilder, PointBuilder, PolygonBuilder};
use geoarrow::datatypes::{Dimension, GeoArrowType, LineStringType, PointType, PolygonType};
use geoarrow_array::GeoArrowArray;
use geoarrow_array::array::from_arrow_array;

use crate::{as_geo_geometries, as_geometry_chunks};

/// Assemble the exploded parts into a list array of length n, so each input row keeps one element holding its own parts.
fn finish_list(
    values: ArrayRef,
    field: Field,
    offsets: Vec<i32>,
    mut nulls: NullBufferBuilder,
) -> extendr_api::Result<Robj> {
    let list = ListArray::try_new(
        Arc::new(field),
        OffsetBuffer::new(offsets.into()),
        values,
        nulls.finish(),
    )
    .map_err(|e| Error::Other(e.to_string()))?;

    list.into_data().into_arrow_robj()
}

/// Split multi-part geometries into their parts
///
/// Returns a list the same length as the input, where each element is an array
/// of that geometry's constituent parts. `ga_explode()` turns a multipolygon of
/// three polygons into one element holding a polygon array of length three.
///
/// @details
/// `ga_explode()` returns the singular form of the input geometry type:
///
/// | input | element type |
/// | --- | --- |
/// | `multipoint`, `point` | `point` |
/// | `multilinestring`, `linestring` | `linestring` |
/// | `multipolygon`, `polygon` | `polygon` |
///
/// `ga_explode()` decides the element type from the array's declared type, not
/// from its contents, so it still returns polygons for an array of
/// multipolygons that all happen to hold one part. `ga_explode()` raises an
/// error for a mixed `geometry` array, which has no single singular type;
/// narrow it with [ga_downcast_geometry()] first.
///
/// `ga_explode()` returns an element of length one for a singular geometry, so
/// it handles any input. For a null geometry, it returns a null element, which
/// is distinct from an empty one.
///
/// Because `ga_explode()` preserves length, you can keep the result alongside
/// the other columns of a table, each element in line with its source row. Use
/// [ga_flatten()] to collapse it into a single array with one row per part.
///
/// @param x a GeoArrow geometry array
/// @returns a list array of the same length as `x`, whose elements are GeoArrow arrays
/// @export
/// @family cast
/// @examplesIf requireNamespace("sf", quietly = TRUE) && requireNamespace("geoarrow", quietly = TRUE)
/// fp <- system.file("shape/nc.shp", package = "sf")
/// nc <- as.data.frame(read_shapefile(fp))
///
/// # one list element per county, holding that county's polygon parts
/// parts <- ga_explode(nc$geometry)
/// parts$length
/// head(lengths(as.vector(parts)))
#[extendr]
fn ga_explode(x: Robj) -> extendr_api::Result<Robj> {
    let chunks = as_geometry_chunks(x)?;
    let n: usize = chunks.iter().map(|c| c.len()).sum();
    let metadata = chunks[0].data_type().metadata().clone();

    let mut geoms: Vec<Option<Geometry<f64>>> = Vec::with_capacity(n);
    for chunk in &chunks {
        geoms.extend(as_geo_geometries(chunk.as_ref())?);
    }

    // the singular type comes from the array's declared type, not its contents
    let kind = match chunks[0].data_type() {
        GeoArrowType::Point(_) | GeoArrowType::MultiPoint(_) => 0u8,
        GeoArrowType::LineString(_) | GeoArrowType::MultiLineString(_) => 1u8,
        GeoArrowType::Polygon(_) | GeoArrowType::MultiPolygon(_) => 2u8,
        other => {
            let name = match other {
                GeoArrowType::Geometry(_) => "geometry",
                GeoArrowType::GeometryCollection(_) => "geometrycollection",
                GeoArrowType::Rect(_) => "rect",
                GeoArrowType::Wkb(_) | GeoArrowType::LargeWkb(_) | GeoArrowType::WkbView(_) => {
                    "wkb"
                }
                _ => "wkt",
            };
            return Err(Error::Other(format!(
                "Cannot explode a {name} array; narrow it to a single geometry type first, \
                 for example with downcast_geometry()"
            )));
        }
    };

    let mut offsets: Vec<i32> = Vec::with_capacity(n + 1);
    offsets.push(0);
    let mut nulls = NullBufferBuilder::new(n);

    match kind {
        0 => {
            let mut bldr = PointBuilder::new(PointType::new(Dimension::XY, metadata.clone()));
            let mut total = 0i32;
            for g in &geoms {
                match g {
                    Some(Geometry::Point(p)) => {
                        bldr.try_push_point(Some(p))
                            .map_err(|e| Error::Other(e.to_string()))?;
                        total += 1;
                        nulls.append(true);
                    }
                    Some(Geometry::MultiPoint(mp)) => {
                        for p in mp {
                            bldr.try_push_point(Some(p))
                                .map_err(|e| Error::Other(e.to_string()))?;
                            total += 1;
                        }
                        nulls.append(true);
                    }
                    _ => nulls.append(false),
                }
                offsets.push(total);
            }
            let arr = bldr.finish();
            let field = arr.data_type().to_field("item", true);
            finish_list(arr.to_array_ref(), field, offsets, nulls)
        }
        1 => {
            let mut bldr =
                LineStringBuilder::new(LineStringType::new(Dimension::XY, metadata.clone()));
            let mut total = 0i32;
            for g in &geoms {
                match g {
                    Some(Geometry::LineString(l)) => {
                        bldr.push_line_string(Some(l))
                            .map_err(|e| Error::Other(e.to_string()))?;
                        total += 1;
                        nulls.append(true);
                    }
                    Some(Geometry::MultiLineString(ml)) => {
                        for l in ml {
                            bldr.push_line_string(Some(l))
                                .map_err(|e| Error::Other(e.to_string()))?;
                            total += 1;
                        }
                        nulls.append(true);
                    }
                    _ => nulls.append(false),
                }
                offsets.push(total);
            }
            let arr = bldr.finish();
            let field = arr.data_type().to_field("item", true);
            finish_list(arr.to_array_ref(), field, offsets, nulls)
        }
        _ => {
            let mut bldr = PolygonBuilder::new(PolygonType::new(Dimension::XY, metadata.clone()));
            let mut total = 0i32;
            for g in &geoms {
                match g {
                    Some(Geometry::Polygon(p)) => {
                        bldr.push_polygon(Some(p))
                            .map_err(|e| Error::Other(e.to_string()))?;
                        total += 1;
                        nulls.append(true);
                    }
                    Some(Geometry::MultiPolygon(mp)) => {
                        for p in mp {
                            bldr.push_polygon(Some(p))
                                .map_err(|e| Error::Other(e.to_string()))?;
                            total += 1;
                        }
                        nulls.append(true);
                    }
                    _ => nulls.append(false),
                }
                offsets.push(total);
            }
            let arr = bldr.finish();
            let field = arr.data_type().to_field("item", true);
            finish_list(arr.to_array_ref(), field, offsets, nulls)
        }
    }
}

/// Collapse a list of geometries into a single array
///
/// The inverse of [ga_explode()]. Concatenates every element's parts into one
/// array, turning a list of `n` elements holding `m` parts in total into an
/// array of length `m`.
///
/// `ga_flatten()` adds nothing for null elements, so it returns a different
/// number of rows than the input whenever an element holds more or fewer than
/// one geometry.
///
/// @param x a list array of GeoArrow arrays, as [ga_explode()] returns
/// @returns a GeoArrow array holding every part, flattened
/// @export
/// @family cast
/// @examplesIf requireNamespace("sf", quietly = TRUE) && requireNamespace("geoarrow", quietly = TRUE)
/// fp <- system.file("shape/nc.shp", package = "sf")
/// nc <- as.data.frame(read_shapefile(fp))
///
/// # 100 counties in, one row per polygon part out
/// length(nc$geometry)
/// ga_flatten(ga_explode(nc$geometry))$length
#[extendr]
fn ga_flatten(x: Robj) -> extendr_api::Result<Robj> {
    let data = arrow::array::ArrayData::from_arrow_robj(&x)
        .map_err(|e| Error::Other(format!("Expected a list array: {e}")))?;
    let array = arrow::array::make_array(data);

    let list = array.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
        Error::Other("Expected a list array, as returned by explode()".to_string())
    })?;

    // the extension metadata lives on the child field, so rebuild through it
    let DataType::List(field) = list.data_type() else {
        return Err(Error::Other("Expected a list array".to_string()));
    };
    let values =
        from_arrow_array(list.values().as_ref(), field).map_err(|e| Error::Other(e.to_string()))?;
    values.into_arrow_robj()
}

extendr_module! {
    mod explode;
    fn ga_explode;
    fn ga_flatten;
}
