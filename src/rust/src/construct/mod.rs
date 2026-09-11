use arrow::array::Array;
use arrow_extendr::IntoArrowRobj;
use extendr_api::prelude::*;
use geo::{LineString, Point};
use geo_traits::to_geo::ToGeoPoint;
use geoarrow::array::{GeoArrowArray, GeoArrowArrayAccessor, LineStringBuilder, PointBuilder};
use geoarrow::datatypes::{Crs, Dimension, LineStringType, Metadata, PointType};
use std::sync::Arc;

use crate::{as_point_chunks, as_recycled_points, try_float_array};

/// Build a point array from x and y coordinates
///
/// Pairs two numeric vectors into a GeoArrow point array. The shorter of the
/// two is recycled when it is length 1.
///
/// @details
/// A row where either coordinate is `NA` gives a null point rather than a
/// point at an arbitrary location, so the result always has the same length as
/// the longer input.
///
/// `crs` is stored verbatim in the array's GeoArrow metadata and is never
/// interpreted, so nothing here reprojects. Anything a reader would produce
/// works, whether an authority code such as `"EPSG:4326"`, WKT, or PROJJSON.
/// Leaving it `NULL` produces an array with no CRS.
///
/// @param x a numeric vector or float64 array of x coordinates
/// @param y a numeric vector or float64 array of y coordinates; length 1 or
///   the same length as `x`
/// @param crs a coordinate reference system to record, or `NULL` for none
/// @returns a GeoArrow point array
/// @export
/// @family construct
/// @examplesIf requireNamespace("geoarrow", quietly = TRUE) && requireNamespace("sf", quietly = TRUE)
/// pts <- ga_xy(c(0, 1, 2), c(0, 1, 4), crs = "EPSG:4326")
/// sf::st_as_sfc(geoarrow::as_geoarrow_vctr(pts))
///
/// # a length 1 coordinate is recycled
/// sf::st_as_sfc(geoarrow::as_geoarrow_vctr(ga_xy(c(0, 1, 2), 0)))
///
/// # NA in either coordinate gives a null point
/// ga_xy(c(0, NA), c(0, 1))$null_count
#[extendr]
fn ga_xy(
    x: Robj,
    y: Robj,
    #[extendr(default = "NULL")] crs: Option<String>,
) -> anyhow::Result<Robj> {
    let xs = try_float_array(x, "x").map_err(|e| anyhow::anyhow!("{e}"))?;
    let ys = try_float_array(y, "y").map_err(|e| anyhow::anyhow!("{e}"))?;

    let n = xs.len().max(ys.len());
    for (label, len) in [("x", xs.len()), ("y", ys.len())] {
        if len != n && len != 1 {
            return Err(anyhow::anyhow!(
                "`{label}` must be length 1 or the same length as the longest argument ({n}), got {len}"
            ));
        }
    }

    let metadata = match crs {
        Some(value) => Arc::new(Metadata::new(Crs::from_unknown_crs_type(value), None)),
        None => Arc::new(Metadata::default()),
    };

    let mut bldr = PointBuilder::new(PointType::new(Dimension::XY, metadata));
    bldr.reserve(n);

    for i in 0..n {
        let xi = if xs.len() == 1 { 0 } else { i };
        let yi = if ys.len() == 1 { 0 } else { i };
        if xs.is_null(xi) || ys.is_null(yi) {
            bldr.push_point(None::<&Point<f64>>);
        } else {
            bldr.push_point(Some(&Point::new(xs.value(xi), ys.value(yi))));
        }
    }

    bldr.finish()
        .into_arrow_robj()
        .map_err(|e| anyhow::anyhow!("{e}"))
}

/// Build a line between pairs of points
///
/// Returns a two point linestring joining each `start` to the matching `end`.
/// `end` is recycled, so one destination pairs with every origin.
///
/// @details
/// A null or empty point on either side gives a null element, so the result is
/// always the same length as `start`. The length of the line is
/// [ga_length_euclidean()], which equals [ga_dist_euclidean_pairwise()] on the
/// same pair.
///
/// @param start a GeoArrow point array
/// @param end a GeoArrow point array; length 1 or the same length as `start`
/// @returns a GeoArrow linestring array of the same length as `start`
/// @export
/// @family construct
/// @examplesIf requireNamespace("geoarrow", quietly = TRUE)
/// start <- ga_xy(c(0, 0), c(0, 3))
/// end <- ga_xy(c(4, 4), c(0, 3))
///
/// as.vector(ga_length_euclidean(ga_make_line(start, end)))
#[extendr]
fn ga_make_line(start: Robj, end: Robj) -> extendr_api::Result<Robj> {
    let starts = as_point_chunks(start)?;
    let n = starts.iter().map(|c| c.len()).sum();
    let ends = as_recycled_points(end, n, "end")?;

    let metadata = starts[0].data_type().metadata().clone();
    let mut bldr = LineStringBuilder::new(LineStringType::new(Dimension::XY, metadata));
    let mut ends = ends.iter().cycle();

    for chunk in &starts {
        for item in chunk.iter() {
            let line = match (item, ends.next().and_then(|e| e.as_ref())) {
                (Some(Ok(s)), Some(e)) => {
                    let s = s.to_point();
                    if s.x().is_finite() && s.y().is_finite() {
                        Some(LineString::new(vec![s.into(), (*e).into()]))
                    } else {
                        None
                    }
                }
                _ => None,
            };
            bldr.push_line_string(line.as_ref())
                .map_err(|e| Error::Other(e.to_string()))?;
        }
    }

    bldr.finish().into_arrow_robj()
}

extendr_module! {
    mod construct;
    fn ga_xy;
    fn ga_make_line;
}
