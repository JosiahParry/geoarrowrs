use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, ArrayData, ArrayRef, DictionaryArray, StringArray, make_array};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Int32Type};
use arrow_extendr::{FromArrowRobj, IntoArrowRobj};
use extendr_api::prelude::*;
use geo::GeometryCollection;
use geoarrow::array::GeometryCollectionBuilder;
use geoarrow::datatypes::{Dimension, GeoArrowType, GeometryCollectionType};
use geoarrow_array::GeoArrowArray;
use geoarrow_cast::downcast::NativeType;

use crate::cast::CastGeoArrow;
use crate::cast::extract::Parts;
use crate::geometry_type::TypeName;
use crate::{as_geo_geometries_par, as_geometry_chunks, try_float_array};

/// Read `sizes` as the run lengths they have to be, covering the whole array.
fn as_group_sizes(sizes: Robj, n: usize) -> extendr_api::Result<Vec<usize>> {
    let values = try_float_array(sizes, "sizes")?;
    let mut out = Vec::with_capacity(values.len());
    let mut total = 0usize;

    for i in 0..values.len() {
        if values.is_null(i) {
            return Err(Error::Other("`sizes` must not contain NA".to_string()));
        }
        let v = values.value(i);
        if v < 0.0 || v.fract() != 0.0 {
            return Err(Error::Other(format!(
                "`sizes` must be whole and not negative, got {v}"
            )));
        }
        total += v as usize;
        out.push(v as usize);
    }

    if total != n {
        return Err(Error::Other(format!(
            "`sizes` must sum to the length of `geometry` ({n}), got {total}"
        )));
    }
    Ok(out)
}

/// Read `by` as one dictionary code per row, using Arrow's own dictionary
/// encoder to assign codes in the order each value first appears. This is one
/// pass over the array with no per-row Rust allocation, unlike hashing a
/// materialized `String` per row ourselves.
pub(crate) fn try_group_codes(
    robj: Robj,
    label: &'static str,
) -> extendr_api::Result<Vec<Option<i32>>> {
    let array: ArrayRef = if let Ok(arr) = ArrayData::from_arrow_robj(&robj) {
        make_array(arr)
    } else if let Ok(strings) = Strings::try_from(&robj) {
        let values: Vec<Option<String>> = strings
            .iter()
            .map(|s| (!s.is_na()).then(|| s.to_string()))
            .collect();
        Arc::new(StringArray::from(values))
    } else if let Ok(numbers) = try_float_array(robj, label) {
        Arc::new(numbers)
    } else {
        return Err(Error::Other(format!(
            "Expected `{label}` to be a character vector, a numeric vector, or an Arrow array"
        )));
    };

    let dict_type = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
    let dict = cast(&array, &dict_type).map_err(|e| Error::Other(e.to_string()))?;
    let dict = dict
        .as_any()
        .downcast_ref::<DictionaryArray<Int32Type>>()
        .ok_or_else(|| Error::Other(format!("Expected `{label}` to cast to a dictionary array")))?;

    Ok(dict.keys().iter().collect())
}

/// Split items into groups by code in order of first appearance, with a null code as its own group and null items dropped.
pub(crate) fn group_by_codes<T>(
    items: Vec<Option<T>>,
    codes: Vec<Option<i32>>,
    label: &str,
) -> extendr_api::Result<Vec<Vec<T>>> {
    if codes.len() != items.len() {
        return Err(Error::Other(format!(
            "`by` must have one label per row of `{label}` ({}), got {}",
            items.len(),
            codes.len()
        )));
    }

    let mut index = HashMap::<Option<i32>, usize>::new();
    let mut groups = Vec::<Vec<T>>::new();

    for (item, code) in items.into_iter().zip(codes) {
        let Some(item) = item else { continue };
        let idx = *index.entry(code).or_insert_with(|| {
            groups.push(Vec::new());
            groups.len() - 1
        });
        groups[idx].push(item);
    }

    Ok(groups)
}

/// Collect an array into one geometry, or one per group
///
/// Gathers the geometries into a single multi geometry or geometry collection,
/// returning an array of length 1, or one per group when you group the rows
/// with `sizes` or `by`. This is the aggregate a `summarise()` wants, not a row
/// by row operation.
///
/// @details
/// `ga_collect_agg()` picks the result type from the type of `geometry`, as
/// PostGIS `ST_Collect()` and `sf::st_combine()` do. It gathers a point or
/// multipoint array into multipoints, a linestring or multilinestring array
/// into multilinestrings, and a polygon, multipolygon, or box array into
/// multipolygons. It gathers a mixed geometry, WKB, or WKT array into geometry
/// collections, since those types can hold any geometry; call
/// [ga_downcast_geometry()] first to get the multi type when every row has one
/// kind.
///
/// `ga_collect_agg()` neither dissolves nor reorders, so it keeps overlapping
/// parts overlapping and keeps every part of every non null input row. Use
/// [ga_unary_union()] to merge overlapping polygons into one outline instead.
///
/// `ga_collect_agg()` reads `sizes` as the number of rows in each group, in
/// order, which you get from a `group_by()` and `summarise(n = n())` over the
/// same ordering. You have to sort the rows by the grouping first, since
/// `ga_collect_agg()` takes the runs in row order: it returns the `i`th run of
/// `sizes` rows as group `i`. It returns nothing about the groups themselves,
/// so keep the keys from the same `summarise()` to say what each row is.
///
/// With `by`, `ga_collect_agg()` groups the rows itself, from an unsorted key
/// of one label per row, so you need no `arrange()` first. It returns one
/// geometry per distinct label, in the order each label first appears in
/// `by`; get the matching keys back with `vctrs::vec_unique(by)`.
/// `ga_collect_agg()` groups rows with `NA` in `by` together like any other
/// label. Give at most one of `sizes` or `by`; with `NULL` for both, it
/// collects everything as one group.
///
/// Being an aggregate, this is one of the few functions here that does not
/// preserve length, and it has no Arrow kernel: a kernel sees one batch at a
/// time and would collect each batch separately.
///
/// @param geometry a GeoArrow geometry array
/// @param sizes the rows in each group, in order, or `NULL` for one group
/// @param by a character vector, numeric vector, or Arrow array with one label
///   per row of `geometry`, or `NULL` to use `sizes` instead
/// @returns a GeoArrow multipoint, multilinestring, multipolygon, or geometry
///   collection array, of length 1 or of one element per group
/// @export
/// @family aggregate
/// @examplesIf requireNamespace("sf", quietly = TRUE) && requireNamespace("geoarrow", quietly = TRUE)
/// nc <- as.data.frame(read_shapefile(
///   system.file("shape/nc.shp", package = "sf")
/// ))
/// pts <- ga_centroid(nc$geometry)
///
/// # every centroid as one geometry, then the shape they span
/// collected <- ga_collect_agg(pts)
/// as.vector(ga_unsigned_area(ga_convex_hull(collected)))
///
/// # the same, in groups of twenty rows
/// grouped <- ga_collect_agg(pts, sizes = rep(20, 5))
/// as.vector(ga_unsigned_area(ga_convex_hull(grouped)))
///
/// # group by an unsorted key, without an arrange() first
/// key <- rep(c("north", "south"), length.out = pts$length)
/// by_key <- ga_collect_agg(pts, by = key)
/// vctrs::vec_unique(key)
#[extendr]
fn ga_collect_agg(
    geometry: Robj,
    #[extendr(default = "NULL")] sizes: Robj,
    #[extendr(default = "NULL")] by: Robj,
) -> extendr_api::Result<Robj> {
    if !sizes.is_null() && !by.is_null() {
        return Err(Error::Other(
            "give at most one of `sizes` or `by`".to_string(),
        ));
    }

    let chunks = as_geometry_chunks(geometry)?;
    let metadata = chunks[0].data_type().metadata().clone();
    // a point, line, or polygon array gathers into the multi type of its kind, anything else into a collection
    let kind = match chunks[0].data_type() {
        GeoArrowType::Point(_) | GeoArrowType::MultiPoint(_) => Some(NativeType::Point),
        GeoArrowType::LineString(_) | GeoArrowType::MultiLineString(_) => {
            Some(NativeType::LineString)
        }
        GeoArrowType::Polygon(_) | GeoArrowType::MultiPolygon(_) | GeoArrowType::Rect(_) => {
            Some(NativeType::Polygon)
        }
        _ => None,
    };
    let geoms = as_geo_geometries_par(crate::threads::Threads::get(), &chunks)?;

    let groups = if by.is_null() {
        let sizes = if sizes.is_null() {
            vec![geoms.len()]
        } else {
            as_group_sizes(sizes, geoms.len())?
        };
        let mut rows = geoms.into_iter();
        sizes
            .into_iter()
            .map(|n| rows.by_ref().take(n).flatten().collect::<Vec<_>>())
            .collect::<Vec<_>>()
    } else {
        group_by_codes(geoms, try_group_codes(by, "by")?, "geometry")?
    };

    if let Some((kind, to_type)) =
        kind.and_then(|kind| Some((kind, TypeName(kind).multi_type(metadata.clone())?)))
    {
        let multis = groups
            .into_iter()
            .map(|group| {
                let mut parts = Parts::default();
                parts.extend(group);
                parts.into_multi(kind)
            })
            .collect::<Vec<_>>();
        return multis.cast_geoarrow(&to_type)?.into_arrow_robj();
    }

    let mut bldr =
        GeometryCollectionBuilder::new(GeometryCollectionType::new(Dimension::XY, metadata));
    for parts in groups {
        bldr.push_geometry_collection(Some(&GeometryCollection(parts)))
            .map_err(|e| Error::Other(e.to_string()))?;
    }
    bldr.finish().into_arrow_robj()
}

extendr_module! {
    mod aggregate;
    fn ga_collect_agg;
}
