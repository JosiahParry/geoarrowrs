use std::sync::Arc;

use arrow::array::{Array, ArrayRef, StructArray};
use arrow::datatypes::{Field, Fields};
use arrow_extendr::IntoArrowRobj;
use extendr_api::prelude::*;
use geo::algorithm::extremes::Outcome;
use geo::{Coord, Extremes};
use geo_traits::to_geo::ToGeoGeometry;
use geoarrow::array::PointBuilder;
use geoarrow::datatypes::{Dimension, PointType};
use geoarrow::error::{GeoArrowError, GeoArrowResult};
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor, downcast_geoarrow_array};

use crate::as_geometry_chunks;

fn impl_extremes<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
) -> GeoArrowResult<Vec<Option<Outcome<f64>>>> {
    let mut out = Vec::with_capacity(array.len());
    for item in array.iter() {
        match item {
            // try_ rather than to_geometry, which panics on an empty point
            Some(geom) => out.push(geom?.try_to_geometry().and_then(|g| g.extremes())),
            None => out.push(None),
        }
    }
    Ok(out)
}

/// Dispatch over whatever concrete geometry array we were handed, so this works
/// on a multipolygon array as readily as on a mixed geometry array.
fn extremes_of(array: &dyn GeoArrowArray) -> GeoArrowResult<Vec<Option<Outcome<f64>>>> {
    downcast_geoarrow_array!(array, impl_extremes)
}

/// Compute the extreme coordinates of geometries
///
/// Returns a struct array with four point fields -- `x_min`, `x_max`, `y_min`
/// and `y_max` -- giving the coordinate that is furthest in each direction. The
/// result has one row per input geometry; a null or empty geometry yields a row
/// of four nulls.
///
/// Note these are the extreme *coordinates*, not the corners of the bounding
/// box: the `x_min` point carries the y value of whichever vertex was
/// leftmost. Use [ga_bounding_rect()] for the envelope.
///
/// @param x a GeoArrow geometry array
/// @returns an Arrow struct array of four point fields, with one row per geometry
/// @export
/// @family boundary
/// @references [Extremes](https://docs.rs/geo/latest/geo/algorithm/extremes/trait.Extremes.html)
/// @examplesIf requireNamespace("sf", quietly = TRUE) && requireNamespace("geoarrow", quietly = TRUE)
/// fp <- system.file("shape/nc.shp", package = "sf")
/// nc <- as.data.frame(read_shapefile(fp))
///
/// # one row per county, four point columns
/// head(as.data.frame(ga_extremes(nc$geometry)), 3)
#[extendr]
fn ga_extremes(x: Robj) -> extendr_api::Result<Robj> {
    let err = |e: GeoArrowError| Error::Other(e.to_string());
    let chunks = as_geometry_chunks(x)?;
    let metadata = chunks[0].data_type().metadata().clone();

    let point_type = || PointType::new(Dimension::XY, metadata.clone());
    let mut x_min = PointBuilder::new(point_type());
    let mut x_max = PointBuilder::new(point_type());
    let mut y_min = PointBuilder::new(point_type());
    let mut y_max = PointBuilder::new(point_type());

    for chunk in &chunks {
        for outcome in extremes_of(chunk.as_ref()).map_err(err)? {
            match outcome {
                Some(o) => {
                    x_min.try_push_coord(Some(&o.x_min.coord)).map_err(err)?;
                    x_max.try_push_coord(Some(&o.x_max.coord)).map_err(err)?;
                    y_min.try_push_coord(Some(&o.y_min.coord)).map_err(err)?;
                    y_max.try_push_coord(Some(&o.y_max.coord)).map_err(err)?;
                }
                None => {
                    let null = None::<&Coord<f64>>;
                    x_min.try_push_coord(null).map_err(err)?;
                    x_max.try_push_coord(null).map_err(err)?;
                    y_min.try_push_coord(null).map_err(err)?;
                    y_max.try_push_coord(null).map_err(err)?;
                }
            }
        }
    }

    let arrays = [
        ("x_min", x_min.finish()),
        ("x_max", x_max.finish()),
        ("y_min", y_min.finish()),
        ("y_max", y_max.finish()),
    ];

    let mut fields: Vec<Arc<Field>> = Vec::with_capacity(4);
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(4);
    for (name, arr) in arrays {
        fields.push(Arc::new(arr.data_type().to_field(name, true)));
        columns.push(arr.to_array_ref());
    }

    let res = StructArray::try_new(Fields::from(fields), columns, None)
        .map_err(|e| Error::Other(e.to_string()))?;

    res.into_data().into_arrow_robj()
}

extendr_module! {
    mod extremes;
    fn ga_extremes;
}
