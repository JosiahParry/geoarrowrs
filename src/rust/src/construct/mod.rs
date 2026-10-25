mod columns;
mod line;
mod polygon;

use arrow::array::{Array, Float64Array};
use arrow::buffer::{NullBuffer, ScalarBuffer};
use arrow_extendr::IntoArrowRobj;
use extendr_api::prelude::*;
use geo::{Coord, LineString, Rect};
use geo_traits::to_geo::ToGeoPoint;
use geoarrow::array::{
    CoordBuffer, GeoArrowArray, GeoArrowArrayAccessor, LineStringBuilder, PointArray,
    PolygonBuilder, SeparatedCoordBuffer,
};
use geoarrow::datatypes::{Crs, Dimension, LineStringType, Metadata, PolygonType};
use std::sync::Arc;

use crate::{as_point_chunks, as_recycled_points, try_float_array};

/// Build a point array from coordinates
///
/// Pairs numeric vectors of coordinates into a GeoArrow point array, adding
/// the z and m dimensions when you supply `z` and `m`. `ga_point()` recycles
/// any coordinate of length 1.
///
/// @details
/// `ga_point()` returns a null point for a row where any coordinate is `NA`,
/// rather than a point at an arbitrary location, so the result always has the
/// same length as the longest input. When every coordinate is full length and
/// has no `NA`, it reuses the arrays without copying them.
///
/// `ga_point()` stores `crs` verbatim in the array's GeoArrow metadata and never
/// interprets it, so it never reprojects. It accepts anything a reader would
/// produce, whether an authority code such as `"EPSG:4326"`, WKT, or PROJJSON.
/// If you leave it `NULL`, `ga_point()` produces an array with no CRS.
///
/// @param x,y numeric vectors or float64 arrays of x and y coordinates; each
///   length 1 or the same length as the longest coordinate
/// @param z,m numeric vectors or float64 arrays of z and m values, or `NULL`
///   to leave out that dimension
/// @param crs a coordinate reference system to record, or `NULL` for none
/// @returns a GeoArrow point array
/// @export
/// @family construct
/// @examplesIf requireNamespace("geoarrow", quietly = TRUE) && requireNamespace("sf", quietly = TRUE)
/// pts <- ga_point(c(0, 1, 2), c(0, 1, 4), crs = "EPSG:4326")
/// sf::st_as_sfc(geoarrow::as_geoarrow_vctr(pts))
///
/// # ga_point() recycles a length 1 coordinate
/// sf::st_as_sfc(geoarrow::as_geoarrow_vctr(ga_point(c(0, 1, 2), 0)))
///
/// # supplying z gives a POINT Z
/// sf::st_as_sfc(geoarrow::as_geoarrow_vctr(ga_point(c(0, 1), c(0, 1), z = c(5, 6))))
///
/// # ga_point() returns a null point for NA in any coordinate
/// ga_point(c(0, NA), c(0, 1))$null_count
#[extendr]
fn ga_point(
    x: Robj,
    y: Robj,
    #[extendr(default = "NULL")] z: Robj,
    #[extendr(default = "NULL")] m: Robj,
    #[extendr(default = "NULL")] crs: Option<String>,
) -> extendr_api::Result<Robj> {
    let CoordArgs {
        columns,
        dim,
        metadata,
    } = CoordArgs::read(x, y, z, m, crs)?;

    let n = columns.iter().map(|(_, c)| c.len()).max().unwrap_or(0);
    for (label, c) in &columns {
        if c.len() != n && c.len() != 1 {
            return Err(Error::Other(format!(
                "`{label}` must be length 1 or the same length as the longest argument ({n}), got {}",
                c.len()
            )));
        }
    }

    // a row with NA in any coordinate is a null point
    let at = |c: &Float64Array, i: usize| if c.len() == 1 { 0 } else { i };
    let valid = (0..n)
        .map(|i| columns.iter().all(|(_, c)| c.is_valid(at(c, i))))
        .collect::<Vec<_>>();

    let buffers = columns
        .iter()
        .map(|(_, c)| {
            // a full length column with no NA is reused as is
            if c.len() == n && c.null_count() == 0 {
                return c.values().clone();
            }
            (0..n)
                .map(|i| {
                    if valid[i] {
                        c.value(at(c, i))
                    } else {
                        f64::NAN
                    }
                })
                .collect::<ScalarBuffer<f64>>()
        })
        .collect::<Vec<_>>();

    let coords =
        SeparatedCoordBuffer::from_vec(buffers, dim).map_err(|e| Error::Other(e.to_string()))?;
    let nulls = valid.contains(&false).then(|| NullBuffer::from(valid));
    PointArray::try_new(CoordBuffer::Separated(coords), nulls, metadata)
        .map_err(|e| Error::Other(e.to_string()))?
        .into_arrow_robj()
}

/// The coordinate arguments of a constructor, `x` and `y` then `z` and `m` when given, with their dimension and CRS.
struct CoordArgs {
    columns: Vec<(&'static str, Float64Array)>,
    dim: Dimension,
    metadata: Arc<Metadata>,
}

impl CoordArgs {
    /// Read each coordinate argument as a float64 array, leaving out a `NULL` `z` or `m`.
    fn read(x: Robj, y: Robj, z: Robj, m: Robj, crs: Option<String>) -> extendr_api::Result<Self> {
        let dim = match (z.is_null(), m.is_null()) {
            (true, true) => Dimension::XY,
            (false, true) => Dimension::XYZ,
            (true, false) => Dimension::XYM,
            (false, false) => Dimension::XYZM,
        };

        let mut columns = vec![
            ("x", try_float_array(x, "x")?),
            ("y", try_float_array(y, "y")?),
        ];
        for (label, values) in [("z", z), ("m", m)] {
            if !values.is_null() {
                columns.push((label, try_float_array(values, label)?));
            }
        }

        let metadata = match crs {
            Some(value) => Arc::new(Metadata::new(Crs::from_unknown_crs_type(value), None)),
            None => Arc::new(Metadata::default()),
        };
        Ok(Self {
            columns,
            dim,
            metadata,
        })
    }
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
/// start <- ga_point(c(0, 0), c(0, 3))
/// end <- ga_point(c(4, 4), c(0, 3))
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
    use columns;
    use line;
    use polygon;
    fn ga_point;
    fn ga_make_line;
    fn ga_make_envelope;
}
