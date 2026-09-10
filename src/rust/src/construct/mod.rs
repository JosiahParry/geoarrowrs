use arrow::array::Array;
use arrow_extendr::IntoArrowRobj;
use extendr_api::prelude::*;
use geo::Point;
use geoarrow::array::PointBuilder;
use geoarrow::datatypes::{Crs, Dimension, Metadata, PointType};
use std::sync::Arc;

use crate::try_float_array;

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
    #[extendr(default = "NULL")] crs: Nullable<String>,
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
        Nullable::NotNull(value) => {
            Arc::new(Metadata::new(Crs::from_unknown_crs_type(value), None))
        }
        Nullable::Null => Arc::new(Metadata::default()),
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

extendr_module! {
    mod construct;
    fn ga_xy;
}
