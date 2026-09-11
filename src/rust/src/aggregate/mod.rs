use arrow::array::Array;
use arrow_extendr::IntoArrowRobj;
use extendr_api::prelude::*;
use geo::GeometryCollection;
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

/// Collect an array into one geometry, or one per group
///
/// Gathers the geometries into a single geometry collection, returning an array
/// of length 1, or one collection per group when `sizes` says how the rows are
/// grouped. This is the aggregate a `summarise()` wants, not a row by row
/// operation.
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
/// `summarise()` to say what each row is. `NULL` collects everything as one
/// group.
///
/// Being an aggregate, this is one of the few functions here that does not
/// preserve length, and it is not registered as an Arrow kernel: a kernel sees
/// one batch at a time and would collect each batch separately.
///
/// @param geometry a GeoArrow geometry array
/// @param sizes the rows in each group, in order, or `NULL` for one group
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
#[extendr]
fn ga_collect_agg(
    geometry: Robj,
    #[extendr(default = "NULL")] sizes: Robj,
) -> extendr_api::Result<Robj> {
    let chunks = as_geometry_chunks(geometry)?;
    let metadata = chunks[0].data_type().metadata().clone();
    let geoms = as_geo_geometries_par(crate::threads::Threads::get(), &chunks)?;

    let groups = if sizes.is_null() {
        vec![geoms.len()]
    } else {
        as_group_sizes(sizes, geoms.len())?
    };

    let mut bldr =
        GeometryCollectionBuilder::new(GeometryCollectionType::new(Dimension::XY, metadata));
    let mut rows = geoms.into_iter();
    for n in groups {
        let parts = rows.by_ref().take(n).flatten().collect::<Vec<_>>();
        bldr.push_geometry_collection(Some(&GeometryCollection(parts)))
            .map_err(|e| Error::Other(e.to_string()))?;
    }

    bldr.finish().into_arrow_robj()
}

extendr_module! {
    mod aggregate;
    fn ga_collect_agg;
}
