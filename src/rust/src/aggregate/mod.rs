use arrow_extendr::IntoArrowRobj;
use extendr_api::prelude::*;
use geo::GeometryCollection;
use geoarrow::array::GeometryCollectionBuilder;
use geoarrow::datatypes::{Dimension, GeometryCollectionType};
use geoarrow_array::GeoArrowArray;

use crate::{as_geo_geometries, as_geometry_chunks};

/// Collect a whole array into one geometry
///
/// Gathers every geometry in the array into a single geometry collection,
/// returning an array of length 1. This is the aggregate a `summarise()` wants,
/// not a row by row operation.
///
/// @details
/// Nothing is dissolved and nothing is reordered, so overlapping parts stay
/// overlapping and the collection holds one member per non null input row. Use
/// [ga_unary_union()] to merge overlapping polygons into one outline instead.
///
/// Being an aggregate, this is one of the few functions here that does not
/// preserve length, and it is not registered as an Arrow kernel: a kernel sees
/// one batch at a time and would collect each batch separately.
///
/// @param geometry a GeoArrow geometry array
/// @returns a GeoArrow geometry collection array of length 1
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
#[extendr]
fn ga_collect_agg(geometry: Robj) -> extendr_api::Result<Robj> {
    let chunks = as_geometry_chunks(geometry)?;
    let metadata = chunks[0].data_type().metadata().clone();

    let mut parts = Vec::new();
    for chunk in &chunks {
        for g in as_geo_geometries(chunk.as_ref())?.into_iter().flatten() {
            parts.push(g);
        }
    }

    let mut bldr =
        GeometryCollectionBuilder::new(GeometryCollectionType::new(Dimension::XY, metadata));
    bldr.push_geometry_collection(Some(&GeometryCollection(parts)))
        .map_err(|e| Error::Other(e.to_string()))?;

    bldr.finish().into_arrow_robj()
}

extendr_module! {
    mod aggregate;
    fn ga_collect_agg;
}
