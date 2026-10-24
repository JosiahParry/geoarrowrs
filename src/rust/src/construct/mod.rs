use arrow::array::Array;
use arrow_extendr::IntoArrowRobj;
use extendr_api::prelude::*;
use geo::{Coord, LineString, Point, Rect};
use geo_traits::to_geo::ToGeoPoint;
use geoarrow::array::{
    GeoArrowArray, GeoArrowArrayAccessor, LineStringBuilder, PointBuilder, PolygonBuilder,
};
use geoarrow::datatypes::{Crs, Dimension, LineStringType, Metadata, PointType, PolygonType};
use std::sync::Arc;

use crate::{as_point_chunks, as_recycled_points, try_float_array};

/// Build a point array from x and y coordinates
///
/// Pairs two numeric vectors into a GeoArrow point array. `ga_xy()` recycles
/// the shorter of the two when it is length 1.
///
/// @details
/// `ga_xy()` returns a null point for a row where either coordinate is `NA`,
/// rather than a point at an arbitrary location, so the result always has the
/// same length as the longer input.
///
/// `ga_xy()` stores `crs` verbatim in the array's GeoArrow metadata and never
/// interprets it, so it never reprojects. It accepts anything a reader would
/// produce, whether an authority code such as `"EPSG:4326"`, WKT, or PROJJSON.
/// If you leave it `NULL`, `ga_xy()` produces an array with no CRS.
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
/// # ga_xy() recycles a length 1 coordinate
/// sf::st_as_sfc(geoarrow::as_geoarrow_vctr(ga_xy(c(0, 1, 2), 0)))
///
/// # ga_xy() returns a null point for NA in either coordinate
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
/// `ga_make_line()` recycles `end`, so you can pair one destination with every
/// origin.
///
/// @details
/// `ga_make_line()` returns a null element for a null or empty point on either
/// side, so the result is always the same length as `start`. The length of the
/// line is [ga_length_euclidean()], which equals [ga_dist_euclidean_pairwise()]
/// on the same pair.
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

/// Build rectangles from their bounds
///
/// Returns the axis aligned rectangle spanning `xmin` to `xmax` and `ymin` to
/// `ymax` on each row, as a polygon array. `ga_make_envelope()` recycles any
/// bound of length 1.
///
/// @details
/// This matches PostGIS `ST_MakeEnvelope()`. `ga_make_envelope()` sorts each
/// pair of bounds, so a row with `xmin` above `xmax` gives the same rectangle.
/// It returns a null polygon for a row where any bound is `NA`, so the result
/// always has the same length as the longest bound.
///
/// `ga_make_envelope()` stores `crs` verbatim in the array's GeoArrow metadata
/// and never interprets it. If you leave it `NULL`, it produces an array with
/// no CRS.
///
/// @param xmin,ymin,xmax,ymax numeric vectors or float64 arrays of the bounds;
///   each length 1 or the same length as the longest
/// @param crs a coordinate reference system to record, or `NULL` for none
/// @returns a GeoArrow polygon array
/// @export
/// @family construct
/// @examplesIf requireNamespace("geoarrow", quietly = TRUE) && requireNamespace("sf", quietly = TRUE)
/// boxes <- ga_make_envelope(c(0, 10), c(0, 10), c(1, 12), c(2, 14))
/// sf::st_as_sfc(geoarrow::as_geoarrow_vctr(boxes))
///
/// as.vector(ga_unsigned_area(boxes))
#[extendr]
fn ga_make_envelope(
    xmin: Robj,
    ymin: Robj,
    xmax: Robj,
    ymax: Robj,
    #[extendr(default = "NULL")] crs: Option<String>,
) -> anyhow::Result<Robj> {
    let x0 = try_float_array(xmin, "xmin").map_err(|e| anyhow::anyhow!("{e}"))?;
    let y0 = try_float_array(ymin, "ymin").map_err(|e| anyhow::anyhow!("{e}"))?;
    let x1 = try_float_array(xmax, "xmax").map_err(|e| anyhow::anyhow!("{e}"))?;
    let y1 = try_float_array(ymax, "ymax").map_err(|e| anyhow::anyhow!("{e}"))?;

    let n = x0.len().max(y0.len()).max(x1.len()).max(y1.len());
    for (label, len) in [
        ("xmin", x0.len()),
        ("ymin", y0.len()),
        ("xmax", x1.len()),
        ("ymax", y1.len()),
    ] {
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

    let mut bldr = PolygonBuilder::new(PolygonType::new(Dimension::XY, metadata));
    for (((a, b), c), d) in x0
        .iter()
        .cycle()
        .zip(y0.iter().cycle())
        .zip(x1.iter().cycle())
        .zip(y1.iter().cycle())
        .take(n)
    {
        let rect = match (a, b, c, d) {
            (Some(a), Some(b), Some(c), Some(d)) => {
                Some(Rect::new(Coord { x: a, y: b }, Coord { x: c, y: d }).to_polygon())
            }
            _ => None,
        };
        bldr.push_polygon(rect.as_ref())
            .map_err(|e| anyhow::anyhow!("{e}"))?;
    }

    bldr.finish()
        .into_arrow_robj()
        .map_err(|e| anyhow::anyhow!("{e}"))
}

extendr_module! {
    mod construct;
    fn ga_xy;
    fn ga_make_line;
    fn ga_make_envelope;
}
