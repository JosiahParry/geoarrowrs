use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, ArrayData, ArrayRef, DictionaryArray, StringArray, make_array};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Int32Type};
use arrow_extendr::{FromArrowRobj, IntoArrowRobj};
use extendr_api::prelude::*;
use geo::{Geometry, GeometryCollection};
use geoarrow::array::GeometryCollectionBuilder;
use geoarrow::datatypes::{Dimension, GeometryCollectionType};
use geoarrow_array::GeoArrowArray;

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
fn try_group_codes(robj: Robj, label: &'static str) -> extendr_api::Result<Vec<Option<i32>>> {
    let array: ArrayRef = if let Ok(arr) = ArrayData::from_arrow_robj(&robj) {
        make_array(arr)
    } else {
        let Ok(strings) = Strings::try_from(&robj) else {
            return Err(Error::Other(format!(
                "Expected `{label}` to be a character vector or an Arrow array"
            )));
        };
        let values: Vec<Option<String>> = strings
            .iter()
            .map(|s| (!s.is_na()).then(|| s.to_string()))
            .collect();
        Arc::new(StringArray::from(values))
    };

    let dict_type = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
    let dict = cast(&array, &dict_type).map_err(|e| Error::Other(e.to_string()))?;
    let dict = dict
        .as_any()
        .downcast_ref::<DictionaryArray<Int32Type>>()
        .ok_or_else(|| Error::Other(format!("Expected `{label}` to cast to a dictionary array")))?;

    Ok(dict.keys().iter().collect())
}

/// Split geometries into groups by code, in the order each code (including a
/// null code, treated as its own group) first appears. Null geometries are
/// dropped, matching the ungrouped path.
fn group_by_codes(
    geoms: Vec<Option<Geometry<f64>>>,
    codes: Vec<Option<i32>>,
) -> extendr_api::Result<Vec<Vec<Geometry<f64>>>> {
    if codes.len() != geoms.len() {
        return Err(Error::Other(format!(
            "`by` must have one label per row of `geometry` ({}), got {}",
            geoms.len(),
            codes.len()
        )));
    }

    let mut index: HashMap<Option<i32>, usize> = HashMap::new();
    let mut groups: Vec<Vec<Geometry<f64>>> = Vec::new();

    for (geom, code) in geoms.into_iter().zip(codes) {
        let Some(geom) = geom else { continue };
        let idx = *index.entry(code).or_insert_with(|| {
            groups.push(Vec::new());
            groups.len() - 1
        });
        groups[idx].push(geom);
    }

    Ok(groups)
}

/// Collect an array into one geometry, or one per group
///
/// Gathers the geometries into a single geometry collection, returning an array
/// of length 1, or one collection per group when `sizes` or `by` says how the
/// rows are grouped. This is the aggregate a `summarise()` wants, not a row by
/// row operation.
///
/// @details
/// Nothing is dissolved and nothing is reordered, so overlapping parts stay
/// overlapping and the collection holds one member per non null input row. Use
/// [ga_unary_union()] to merge overlapping polygons into one outline instead.
///
/// `sizes` is the number of rows in each group, in order, which is what a
/// `group_by()` and `summarise(n = n())` over the same ordering gives. The rows
/// have to already be sorted by the grouping, since the runs are taken as they
/// come: group `i` of the result is the `i`th run of `sizes` rows. Nothing is
/// returned about the groups themselves, so keep the keys from the same
/// `summarise()` to say what each row is.
///
/// `by` groups the rows itself, from an unsorted key of one label per row, so
/// no `arrange()` is needed first. The result has one collection per distinct
/// label, in the order each label first appears in `by`; get the matching keys
/// back with `vctrs::vec_unique(by)`. `NA` in `by` groups those rows together
/// like any other label. Give at most one of `sizes` or `by`; `NULL` for both
/// collects everything as one group.
///
/// Being an aggregate, this is one of the few functions here that does not
/// preserve length, and it is not registered as an Arrow kernel: a kernel sees
/// one batch at a time and would collect each batch separately.
///
/// @param geometry a GeoArrow geometry array
/// @param sizes the rows in each group, in order, or `NULL` for one group
/// @param by a character vector or Arrow array with one label per row of
///   `geometry`, or `NULL` to use `sizes` instead
/// @returns a GeoArrow geometry collection array, of length 1 or of one element
///   per group
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
/// # grouped by an unsorted key, no arrange() needed first
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
    let geoms = as_geo_geometries_par(crate::threads::Threads::get(), &chunks)?;

    let mut bldr =
        GeometryCollectionBuilder::new(GeometryCollectionType::new(Dimension::XY, metadata));

    if by.is_null() {
        let groups = if sizes.is_null() {
            vec![geoms.len()]
        } else {
            as_group_sizes(sizes, geoms.len())?
        };

        let mut rows = geoms.into_iter();
        for n in groups {
            let parts = rows.by_ref().take(n).flatten().collect::<Vec<_>>();
            bldr.push_geometry_collection(Some(&GeometryCollection(parts)))
                .map_err(|e| Error::Other(e.to_string()))?;
        }
    } else {
        let codes = try_group_codes(by, "by")?;
        for parts in group_by_codes(geoms, codes)? {
            bldr.push_geometry_collection(Some(&GeometryCollection(parts)))
                .map_err(|e| Error::Other(e.to_string()))?;
        }
    }

    bldr.finish().into_arrow_robj()
}

extendr_module! {
    mod aggregate;
    fn ga_collect_agg;
}
