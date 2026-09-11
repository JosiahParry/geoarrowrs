mod concave_hull;
mod extremes;

use arrow_extendr::IntoArrowRobj;
use extendr_api::prelude::*;
use geo::{BoundingRect, ConvexHull, MinimumRotatedRect, Polygon};
use geoarrow::{
    array::{PolygonBuilder, RectBuilder},
    datatypes::{BoxType, Dimension, PolygonType},
};
use geoarrow_array::GeoArrowArray;

use crate::{as_geo_geometries, as_geometry_chunks};

/// Axis-aligned bounding rectangle
///
/// Returns the smallest axis-aligned rectangle that contains each geometry.
///
/// @param x a GeoArrow geometry array
/// @returns a GeoArrow rect array
/// @export
/// @family boundary
/// @references [BoundingRect](https://docs.rs/geo/latest/geo/algorithm/bounding_rect/trait.BoundingRect.html)
/// @examplesIf requireNamespace("sf", quietly = TRUE) && requireNamespace("geoarrow", quietly = TRUE)
/// fp <- system.file("shape/nc.shp", package = "sf")
/// nc <- as.data.frame(read_shapefile(fp))
///
/// head(geoarrow::as_geoarrow_vctr(ga_bounding_rect(nc$geometry)), 3)
#[extendr]
fn ga_bounding_rect(x: Robj) -> extendr_api::Result<Robj> {
    let chunks = as_geometry_chunks(x)?;
    let n = chunks.iter().map(|c| c.len()).sum();
    let metadata = chunks[0].data_type().metadata().clone();
    let mut bldr = RectBuilder::new(BoxType::new(geoarrow::datatypes::Dimension::XY, metadata));
    bldr.reserve(n);

    for chunk in &chunks {
        for geom in as_geo_geometries(chunk.as_ref())? {
            if let Some(val) = geom {
                bldr.push_rect(val.bounding_rect().as_ref());
            } else {
                bldr.push_rect(None::<&geo::Rect<f64>>);
            }
        }
    }

    bldr.finish().into_arrow_robj()
}

/// Minimum rotated bounding rectangle
///
/// Returns the smallest rectangle of arbitrary rotation that contains each
/// geometry, as a polygon array.
///
/// @param x a GeoArrow geometry array
/// @returns a GeoArrow polygon array
/// @export
/// @family boundary
/// @references [MinimumRotatedRect](https://docs.rs/geo/latest/geo/algorithm/minimum_rotated_rect/trait.MinimumRotatedRect.html)
/// @examplesIf requireNamespace("sf", quietly = TRUE) && requireNamespace("geoarrow", quietly = TRUE)
/// fp <- system.file("shape/nc.shp", package = "sf")
/// nc <- as.data.frame(read_shapefile(fp))
///
/// # free to rotate, so it hugs the county tighter than the envelope
/// rot <- ga_minimum_rotated_rect(nc$geometry)
/// head(as.vector(ga_unsigned_area(rot)), 3)
/// head(as.vector(ga_unsigned_area(ga_bounding_rect(nc$geometry))), 3)
#[extendr]
fn ga_minimum_rotated_rect(x: Robj) -> extendr_api::Result<Robj> {
    let chunks = as_geometry_chunks(x)?;
    let metadata = chunks[0].data_type().metadata().clone();
    let mut bldr = PolygonBuilder::new(PolygonType::new(Dimension::XY, metadata));

    for chunk in &chunks {
        for geom in as_geo_geometries(chunk.as_ref())? {
            if let Some(val) = geom {
                bldr.push_polygon(val.minimum_rotated_rect().as_ref())
                    .map_err(|e| Error::Other(e.to_string()))?;
            } else {
                bldr.push_polygon(None::<&Polygon<f64>>)
                    .map_err(|e| Error::Other(e.to_string()))?;
            }
        }
    }

    bldr.finish().into_arrow_robj()
}

/// Compute the convex hull of geometries
///
/// Returns the smallest convex polygon that contains each geometry.
///
/// @param x a GeoArrow geometry array
/// @returns a GeoArrow polygon array
/// @export
/// @family boundary
/// @references [ConvexHull](https://docs.rs/geo/latest/geo/algorithm/convex_hull/trait.ConvexHull.html)
/// @examplesIf requireNamespace("sf", quietly = TRUE) && requireNamespace("geoarrow", quietly = TRUE)
/// fp <- system.file("shape/nc.shp", package = "sf")
/// nc <- as.data.frame(read_shapefile(fp))
///
/// # the hull fills in every concavity, so it is never smaller
/// head(as.vector(ga_unsigned_area(ga_convex_hull(nc$geometry))), 3)
/// head(as.vector(ga_unsigned_area(nc$geometry)), 3)
#[extendr]
fn ga_convex_hull(x: Robj) -> extendr_api::Result<Robj> {
    let chunks = as_geometry_chunks(x)?;
    let metadata = chunks[0].data_type().metadata().clone();
    let mut bldr = PolygonBuilder::new(PolygonType::new(Dimension::XY, metadata));

    for chunk in &chunks {
        for geom in as_geo_geometries(chunk.as_ref())? {
            if let Some(val) = geom {
                bldr.push_polygon(Some(&val.convex_hull()))
                    .map_err(|e| Error::Other(e.to_string()))?;
            } else {
                bldr.push_polygon(None::<&Polygon<f64>>)
                    .map_err(|e| Error::Other(e.to_string()))?;
            }
        }
    }

    bldr.finish().into_arrow_robj()
}

extendr_module! {
    mod boundary;
    use concave_hull;
    use extremes;
    fn ga_bounding_rect;
    fn ga_minimum_rotated_rect;
    fn ga_convex_hull;
}
