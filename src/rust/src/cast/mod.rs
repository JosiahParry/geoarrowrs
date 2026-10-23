mod explode;

use std::sync::Arc;

use arrow_extendr::IntoArrowRobj;
use extendr_api::prelude::*;
use geo::{Geometry, MultiLineString, MultiPolygon};
use geoarrow::array::from_arrow_array;
use geoarrow::array::{MultiLineStringBuilder, MultiPolygonBuilder};
use geoarrow::datatypes::{
    BoxType, Crs, Dimension, GeoArrowType, GeometryCollectionType, GeometryType, LineStringType,
    Metadata, MultiLineStringType, MultiPointType, MultiPolygonType, PointType, PolygonType,
    WkbType, WktType,
};
use geoarrow_array::GeoArrowArray;
use geoarrow_array::builder::GeometryBuilder;
use geoarrow_cast::cast::cast;
use geoarrow_cast::downcast::{NativeType, infer_downcast_type};
use rayon::prelude::*;

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

/// Stitch the cast pieces back into the single array the R side expects.
pub(crate) fn concat_cast(
    parts: Vec<Arc<dyn GeoArrowArray>>,
    to_type: &GeoArrowType,
) -> extendr_api::Result<Robj> {
    let mut parts = parts;
    if parts.len() == 1 {
        let only = parts
            .pop()
            .ok_or_else(|| Error::Other("empty".to_string()))?;
        // return the geoarrow array itself; ArrayData alone would drop the extension metadata
        return only.into_arrow_robj();
    }

    let arrays = parts.iter().map(|p| p.to_array_ref()).collect::<Vec<_>>();
    let refs = arrays.iter().map(|a| a.as_ref()).collect::<Vec<_>>();
    let combined = arrow::compute::concat(&refs).map_err(|e| Error::Other(e.to_string()))?;
    let field = to_type.to_field("", true);
    from_arrow_array(combined.as_ref(), &field)
        .map_err(|e| Error::Other(e.to_string()))?
        .into_arrow_robj()
}

/// Cast geo geometries to a GeoArrow array of a given type.
pub(crate) trait CastGeoArrow {
    fn cast_geoarrow(&self, to_type: &GeoArrowType) -> extendr_api::Result<Arc<dyn GeoArrowArray>>;
}

/// Goes by way of a mixed array, so one path serves every output type.
impl CastGeoArrow for [Option<Geometry<f64>>] {
    fn cast_geoarrow(&self, to_type: &GeoArrowType) -> extendr_api::Result<Arc<dyn GeoArrowArray>> {
        let mixed = GeometryBuilder::from_nullable_geometries(
            self,
            GeometryType::new(to_type.metadata().clone()),
        )
        .map_err(|e| Error::Other(e.to_string()))?
        .finish();

        cast(&mixed, to_type).map_err(|e| Error::Other(e.to_string()))
    }
}

fn cast_chunks(
    chunks: &[Arc<dyn GeoArrowArray>],
    to_type: &GeoArrowType,
) -> extendr_api::Result<Robj> {
    if chunks.is_empty() {
        return Err(Error::Other("empty array".to_string()));
    }

    let slices = crate::par_slices(chunks);
    let parts = crate::threads::with_pool(crate::threads::Threads::get(), || {
        slices
            .par_iter()
            .map(|slice| cast(slice.as_ref(), to_type))
            .collect::<Result<Vec<_>, _>>()
    })
    .map_err(|e| Error::Other(e.to_string()))?;

    concat_cast(parts, to_type)
}

/// Cast geometries to another GeoArrow geometry type
///
/// Changes the geometry type of an array without changing the geometries it
/// holds.
///
/// @details
/// `ga_cast_geometry()` always succeeds for these casts:
///
/// | from | to |
/// | --- | --- |
/// | `point` | `multipoint` |
/// | `linestring` | `multilinestring` |
/// | `polygon` | `multipolygon` |
/// | any type | `geometry`, `wkb`, `wkt` |
///
/// Others are fallible, and `ga_cast_geometry()` errors when a geometry does
/// not fit the target type, such as when you cast a `multipoint` holding two
/// points to `point`:
///
/// | from | to |
/// | --- | --- |
/// | `multipoint` | `point` |
/// | `multilinestring` | `linestring` |
/// | `multipolygon` | `polygon` |
/// | `geometry` | any concrete type |
///
/// `ga_cast_geometry()` carries the dimension over from the input and does not
/// support casting between different dimensions.
///
/// @param x a GeoArrow geometry array
/// @param to the target type, one of `"point"`, `"linestring"`, `"polygon"`,
///   `"multipoint"`, `"multilinestring"`, `"multipolygon"`,
///   `"geometrycollection"`, `"geometry"`, `"wkb"`, or `"wkt"`
/// @returns a GeoArrow array of the requested type, the same length as `x`
/// @export
/// @family cast
/// @examplesIf requireNamespace("sf", quietly = TRUE) && requireNamespace("geoarrow", quietly = TRUE)
/// pts <- ga_xy(c(0, 1), c(0, 1))
///
/// # you can always widen
/// ga_cast_geometry(pts, "multipoint")
///
/// # and you can always go out to wkb, which is what plain Parquet stores
/// ga_cast_geometry(pts, "wkb")
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

/// Cast to the narrowest geometry type
///
/// Inspects the geometries and casts to the most specific type that can hold
/// every one of them. `ga_downcast_geometry()` turns a `geometry` array holding
/// only points into a `point` array, and leaves one holding points and polygons
/// alone, since no narrower type fits.
///
/// It is the inverse of casting up to `geometry`, and is useful after you read
/// a format that does not record a single geometry type.
///
/// @param x a GeoArrow geometry array
/// @returns a GeoArrow array of the narrowest type that fits, the same length as `x`
/// @export
/// @family cast
/// @examplesIf requireNamespace("sf", quietly = TRUE) && requireNamespace("geoarrow", quietly = TRUE)
/// pts <- ga_xy(c(0, 1), c(0, 1))
/// wide <- ga_cast_geometry(pts, "geometry")
///
/// # only points in there, so you get a point array back
/// ga_downcast_geometry(wide)
#[extendr]
fn ga_downcast_geometry(x: Robj) -> extendr_api::Result<Robj> {
    let chunks = as_geometry_chunks(x)?;
    let metadata = chunks[0].data_type().metadata().clone();
    downcast_chunks(&chunks, metadata)
}

/// Read well known binary as a GeoArrow array
///
/// Parses a binary column of WKB into the narrowest GeoArrow type that fits
/// every geometry in it. `ga_from_wkb()` turns a column of points into a
/// `point` array.
///
/// @details
/// GeoParquet writers, and plenty of plain Parquet ones, store geometry as a
/// bare `binary` column with no GeoArrow extension metadata on it. Most
/// geoarrowrs functions read that directly, but the ones that need a specific
/// geometry type, such as [ga_dist_euclidean_pairwise()], cannot tell what is
/// in it. If you convert once up front, you settle the type for everything
/// downstream.
///
/// `ga_from_wkb()` stores `crs` verbatim in the array's metadata and never
/// interprets it, so it never reprojects. If you leave it `NULL`,
/// `ga_from_wkb()` keeps whatever the input carried, which for a bare binary
/// column is no CRS at all.
///
/// `ga_from_wkb()` leaves mixed geometry types as a `geometry` array, since no
/// narrower type fits.
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
/// Each of these casts to the single type in its name. Because the result
/// type does not depend on the data, these are the casts Arrow can run inside
/// a query, which [ga_from_wkb()] cannot.
///
/// @details
/// Parsing WKB is the expensive part of using a plain Parquet geometry column,
/// and every function that needs a concrete geometry type pays for it on its
/// own. If you cast once up front, you run the rest of the query on native
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
/// The cast is fallible. These functions error, rather than return a null,
/// when a geometry does not fit the target type, such as when you cast a two
/// point multipoint to `point`.
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
