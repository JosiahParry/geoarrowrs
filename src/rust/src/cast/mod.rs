mod explode;

use std::sync::Arc;

use arrow_extendr::IntoArrowRobj;
use extendr_api::prelude::*;
use geo::{Geometry, MultiLineString, MultiPolygon};
use geoarrow::array::{MultiLineStringBuilder, MultiPolygonBuilder};
use geoarrow::datatypes::{
    BoxType, Crs, Dimension, GeoArrowType, GeometryCollectionType, GeometryType, LineStringType,
    Metadata, MultiLineStringType, MultiPointType, MultiPolygonType, PointType, PolygonType,
    WkbType, WktType,
};
use geoarrow_array::GeoArrowArray;
use geoarrow_cast::cast::cast;
use geoarrow_cast::downcast::{NativeType, infer_downcast_type};

use crate::as_geometry_chunks;

/// Build the requested target type, reusing the source's dimension and metadata since `cast` requires the metadata to match.
fn target_type(to: &str, from: &GeoArrowType) -> extendr_api::Result<GeoArrowType> {
    let metadata = from.metadata().clone();
    let dim = from.dimension().unwrap_or(Dimension::XY);

    let ty = match to {
        "point" => GeoArrowType::Point(PointType::new(dim, metadata)),
        "linestring" => GeoArrowType::LineString(LineStringType::new(dim, metadata)),
        "polygon" => GeoArrowType::Polygon(PolygonType::new(dim, metadata)),
        "multipoint" => GeoArrowType::MultiPoint(MultiPointType::new(dim, metadata)),
        "multilinestring" => GeoArrowType::MultiLineString(MultiLineStringType::new(dim, metadata)),
        "multipolygon" => GeoArrowType::MultiPolygon(MultiPolygonType::new(dim, metadata)),
        "geometrycollection" => {
            GeoArrowType::GeometryCollection(GeometryCollectionType::new(dim, metadata))
        }
        "geometry" => GeoArrowType::Geometry(GeometryType::new(metadata)),
        "wkb" => GeoArrowType::Wkb(WkbType::new(metadata)),
        "wkt" => GeoArrowType::Wkt(WktType::new(metadata)),
        other => {
            return Err(Error::Other(format!(
                "`to` must be one of \"point\", \"linestring\", \"polygon\", \"multipoint\", \
                 \"multilinestring\", \"multipolygon\", \"geometrycollection\", \"geometry\", \
                 \"wkb\", or \"wkt\", got \"{other}\""
            )));
        }
    };
    Ok(ty)
}

fn cast_chunks(
    chunks: &[Arc<dyn GeoArrowArray>],
    to_type: &GeoArrowType,
) -> extendr_api::Result<Robj> {
    let mut out: Vec<Arc<dyn GeoArrowArray>> = Vec::with_capacity(chunks.len());
    for chunk in chunks {
        out.push(cast(chunk.as_ref(), to_type).map_err(|e| Error::Other(e.to_string()))?);
    }
    // a single chunk keeps the round trip simple; chunked output waits on arrow-extendr
    let first = out
        .into_iter()
        .next()
        .ok_or_else(|| Error::Other("empty array".to_string()))?;
    // return the geoarrow array itself; ArrayData alone would drop the extension metadata
    first.into_arrow_robj()
}

/// Cast geometries to another GeoArrow geometry type
///
/// Changes the geometry type of an array without changing the geometries it
/// holds.
///
/// @details
/// Some casts always succeed:
///
/// | from | to |
/// | --- | --- |
/// | `point` | `multipoint` |
/// | `linestring` | `multilinestring` |
/// | `polygon` | `multipolygon` |
/// | any type | `geometry`, `wkb`, `wkt` |
///
/// Others are fallible and error when a geometry does not fit the target type,
/// such as a `multipoint` holding two points cast to `point`:
///
/// | from | to |
/// | --- | --- |
/// | `multipoint` | `point` |
/// | `multilinestring` | `linestring` |
/// | `multipolygon` | `polygon` |
/// | `geometry` | any concrete type |
///
/// The dimension is carried over from the input, and casting between different
/// dimensions is not supported.
///
/// @param x a GeoArrow geometry array
/// @param to the target type, one of `"point"`, `"linestring"`, `"polygon"`,
///   `"multipoint"`, `"multilinestring"`, `"multipolygon"`,
///   `"geometrycollection"`, `"geometry"`, `"wkb"`, or `"wkt"`
/// @returns a GeoArrow array of the requested type, the same length as `x`
/// @export
/// @family cast
#[extendr]
fn ga_cast_geometry(x: Robj, to: &str) -> extendr_api::Result<Robj> {
    let chunks = as_geometry_chunks(x)?;
    let from_type = chunks[0].data_type();
    let to_type = target_type(to, &from_type)?;

    // geoarrow-array 0.8's From impls size geom_offsets by coordinate count
    // rather than geometry count, so these two promotions panic upstream
    if matches!(
        (&from_type, &to_type),
        (
            GeoArrowType::LineString(_),
            GeoArrowType::MultiLineString(_)
        ) | (GeoArrowType::Polygon(_), GeoArrowType::MultiPolygon(_))
    ) {
        return promote_to_multi(&chunks, &to_type);
    }

    cast_chunks(&chunks, &to_type)
}

/// Promote singular geometries to their multi form through builders, avoiding the broken upstream From impls.
fn promote_to_multi(
    chunks: &[Arc<dyn GeoArrowArray>],
    to_type: &GeoArrowType,
) -> extendr_api::Result<Robj> {
    let metadata = to_type.metadata().clone();
    let err = |e: geoarrow::error::GeoArrowError| Error::Other(e.to_string());

    let mut geoms = Vec::new();
    for chunk in chunks {
        geoms.extend(crate::as_geo_geometries(chunk.as_ref())?);
    }

    if matches!(to_type, GeoArrowType::MultiLineString(_)) {
        let mut bldr =
            MultiLineStringBuilder::new(MultiLineStringType::new(Dimension::XY, metadata));
        for g in geoms {
            match g {
                Some(Geometry::LineString(l)) => {
                    bldr.push_multi_line_string(Some(&MultiLineString(vec![l])))
                }
                Some(Geometry::MultiLineString(ml)) => bldr.push_multi_line_string(Some(&ml)),
                _ => bldr.push_multi_line_string(None::<&MultiLineString<f64>>),
            }
            .map_err(err)?;
        }
        return bldr.finish().into_arrow_robj();
    }

    let mut bldr = MultiPolygonBuilder::new(MultiPolygonType::new(Dimension::XY, metadata));
    for g in geoms {
        match g {
            Some(Geometry::Polygon(p)) => bldr.push_multi_polygon(Some(&MultiPolygon(vec![p]))),
            Some(Geometry::MultiPolygon(mp)) => bldr.push_multi_polygon(Some(&mp)),
            _ => bldr.push_multi_polygon(None::<&MultiPolygon<f64>>),
        }
        .map_err(err)?;
    }
    bldr.finish().into_arrow_robj()
}

/// Cast geometries to the narrowest type that fits them
///
/// Inspects the geometries and casts to the most specific type that can hold
/// every one of them. A `geometry` array holding only points becomes a `point`
/// array; one holding points and polygons is left alone, since no narrower
/// type fits.
///
/// This is the inverse of casting up to `geometry`, and is useful after
/// reading a format that does not record a single geometry type.
///
/// @param x a GeoArrow geometry array
/// @returns a GeoArrow array of the narrowest type that fits, the same length as `x`
/// @export
/// @family cast
#[extendr]
fn ga_downcast_geometry(x: Robj) -> extendr_api::Result<Robj> {
    let chunks = as_geometry_chunks(x)?;
    let metadata = chunks[0].data_type().metadata().clone();
    downcast_chunks(&chunks, metadata)
}

/// Read well known binary as a GeoArrow array
///
/// Parses a binary column of WKB into the narrowest GeoArrow type that fits
/// every geometry in it. A column of points becomes a `point` array.
///
/// @details
/// GeoParquet writers, and plenty of plain Parquet ones, store geometry as a
/// bare `binary` column with no GeoArrow extension metadata on it. Most
/// geoarrowrs functions read that directly, but the ones that need a specific
/// geometry type, such as [ga_dist_euclidean_pairwise()], cannot tell what is
/// in it. Converting once up front settles the type for everything
/// downstream.
///
/// `crs` is stored verbatim in the array's metadata and is never interpreted,
/// so nothing here reprojects. Leaving it `NULL` keeps whatever the input
/// carried, which for a bare binary column is no CRS at all.
///
/// Mixed geometry types are left as a `geometry` array, since no narrower
/// type fits.
///
/// @param x a binary array of WKB, or any GeoArrow array
/// @param crs a coordinate reference system to record, or `NULL` to keep the
///   one `x` already has
/// @returns a GeoArrow array of the narrowest type that fits, the same length
///   as `x`
/// @export
/// @family cast
/// @examplesIf requireNamespace("geoarrow", quietly = TRUE) && requireNamespace("sf", quietly = TRUE) && requireNamespace("wk", quietly = TRUE) && requireNamespace("arrow", quietly = TRUE)
/// sfc <- sf::st_sfc(sf::st_point(c(0, 0)), sf::st_point(c(3, 4)))
/// wkb <- nanoarrow::as_nanoarrow_array(
///   arrow::Array$create(unclass(wk::as_wkb(sfc)), type = arrow::binary())
/// )
///
/// pts <- ga_from_wkb(wkb, crs = "EPSG:4326")
/// sf::st_as_sfc(geoarrow::as_geoarrow_vctr(pts))
#[extendr]
fn ga_from_wkb(
    x: Robj,
    #[extendr(default = "NULL")] crs: Option<String>,
) -> extendr_api::Result<Robj> {
    let chunks = as_geometry_chunks(x)?;
    // cast refuses to change metadata, so the crs goes on before the downcast rather than after
    let chunks = match crs {
        Some(value) => {
            let metadata = Arc::new(Metadata::new(Crs::from_unknown_crs_type(value), None));
            chunks
                .into_iter()
                .map(|chunk| chunk.with_metadata(metadata.clone()))
                .collect::<Vec<_>>()
        }
        None => chunks,
    };

    let metadata = chunks[0].data_type().metadata().clone();
    downcast_chunks(&chunks, metadata)
}

/// Cast chunks to the narrowest native type that fits them, or leave them alone when none does.
fn downcast_chunks(
    chunks: &[Arc<dyn GeoArrowArray>],
    metadata: Arc<Metadata>,
) -> extendr_api::Result<Robj> {
    let inferred = infer_downcast_type(chunks.iter().map(|c| c.as_ref()))
        .map_err(|e| Error::Other(e.to_string()))?;

    let Some((native, dim)) = inferred else {
        // no single native type fits, so leave the array as it is
        return cast_chunks(chunks, &chunks[0].data_type());
    };

    let to_type = match native {
        NativeType::Point => GeoArrowType::Point(PointType::new(dim, metadata)),
        NativeType::LineString => GeoArrowType::LineString(LineStringType::new(dim, metadata)),
        NativeType::Polygon => GeoArrowType::Polygon(PolygonType::new(dim, metadata)),
        NativeType::MultiPoint => GeoArrowType::MultiPoint(MultiPointType::new(dim, metadata)),
        NativeType::MultiLineString => {
            GeoArrowType::MultiLineString(MultiLineStringType::new(dim, metadata))
        }
        NativeType::MultiPolygon => {
            GeoArrowType::MultiPolygon(MultiPolygonType::new(dim, metadata))
        }
        NativeType::GeometryCollection => {
            GeoArrowType::GeometryCollection(GeometryCollectionType::new(dim, metadata))
        }
        NativeType::Rect => GeoArrowType::Rect(BoxType::new(dim, metadata)),
    };
    cast_chunks(chunks, &to_type)
}

/// Cast to one GeoArrow geometry type
///
/// Each of these casts to the single type its name gives. Because the result
/// type does not depend on the data, these are the casts Arrow can run inside
/// a query, which [ga_from_wkb()] cannot.
///
/// @details
/// Parsing WKB is the expensive part of using a plain Parquet geometry column,
/// and every function that needs a concrete geometry type pays for it on its
/// own. Casting once up front means the rest of the query runs on native
/// GeoArrow instead:
///
/// ```r
/// trip |>
///   mutate(pickup = ga_as_point(t_pickuploc)) |>
///   mutate(
///     d = ga_dist_euclidean_pairwise(pickup, dropoff),
///     b = ga_bearing_euclidean(pickup, dropoff)
///   )
/// ```
///
/// The cast is fallible. A geometry that does not fit the target type, such as
/// a two point multipoint cast to `point`, is an error rather than a null.
///
/// @param x a binary array of WKB, or any GeoArrow array
/// @returns a GeoArrow array of the named type, the same length as `x`
/// @export
/// @rdname ga_as_point
/// @family cast
/// @examplesIf requireNamespace("geoarrow", quietly = TRUE) && requireNamespace("sf", quietly = TRUE) && requireNamespace("wk", quietly = TRUE) && requireNamespace("arrow", quietly = TRUE)
/// sfc <- sf::st_sfc(sf::st_point(c(0, 0)), sf::st_point(c(3, 4)))
/// wkb <- nanoarrow::as_nanoarrow_array(
///   arrow::Array$create(unclass(wk::as_wkb(sfc)), type = arrow::binary())
/// )
///
/// pts <- ga_as_point(wkb)
/// sf::st_as_sfc(geoarrow::as_geoarrow_vctr(pts))
#[extendr]
fn ga_as_point(x: Robj) -> extendr_api::Result<Robj> {
    ga_cast_geometry(x, "point")
}

/// @export
/// @rdname ga_as_point
/// @family cast
#[extendr]
fn ga_as_linestring(x: Robj) -> extendr_api::Result<Robj> {
    ga_cast_geometry(x, "linestring")
}

/// @export
/// @rdname ga_as_point
/// @family cast
#[extendr]
fn ga_as_polygon(x: Robj) -> extendr_api::Result<Robj> {
    ga_cast_geometry(x, "polygon")
}

/// @export
/// @rdname ga_as_point
/// @family cast
#[extendr]
fn ga_as_multipoint(x: Robj) -> extendr_api::Result<Robj> {
    ga_cast_geometry(x, "multipoint")
}

/// @export
/// @rdname ga_as_point
/// @family cast
#[extendr]
fn ga_as_multilinestring(x: Robj) -> extendr_api::Result<Robj> {
    ga_cast_geometry(x, "multilinestring")
}

/// @export
/// @rdname ga_as_point
/// @family cast
#[extendr]
fn ga_as_multipolygon(x: Robj) -> extendr_api::Result<Robj> {
    ga_cast_geometry(x, "multipolygon")
}

extendr_module! {
    mod cast;
    use explode;
    fn ga_cast_geometry;
    fn ga_downcast_geometry;
    fn ga_from_wkb;
    fn ga_as_point;
    fn ga_as_linestring;
    fn ga_as_polygon;
    fn ga_as_multipoint;
    fn ga_as_multilinestring;
    fn ga_as_multipolygon;
}
