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

/// Compute the axis-aligned bounding rectangle of geometries
///
/// Returns the smallest axis-aligned rectangle that contains each geometry.
///
/// @param x a GeoArrow geometry array
/// @returns a GeoArrow rect array
/// @export
/// @family boundary
/// @references [BoundingRect](https://docs.rs/geo/latest/geo/algorithm/bounding_rect/trait.BoundingRect.html)
#[extendr]
fn bounding_rect(x: Robj) -> extendr_api::Result<Robj> {
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

/// Compute the minimum rotated bounding rectangle of geometries
///
/// Returns the smallest rectangle of arbitrary rotation that contains each
/// geometry, as a polygon array.
///
/// @param x a GeoArrow geometry array
/// @returns a GeoArrow polygon array
/// @export
/// @family boundary
/// @references [MinimumRotatedRect](https://docs.rs/geo/latest/geo/algorithm/minimum_rotated_rect/trait.MinimumRotatedRect.html)
#[extendr]
fn minimum_rotated_rect(x: Robj) -> extendr_api::Result<Robj> {
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
#[extendr]
fn convex_hull(x: Robj) -> extendr_api::Result<Robj> {
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
    fn bounding_rect;
    fn minimum_rotated_rect;
    fn convex_hull;
}
