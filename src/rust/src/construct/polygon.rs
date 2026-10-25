use std::collections::HashSet;
use std::ops::Range;
use std::sync::Arc;

use arrow::array::{Array, Float64Array};
use arrow::buffer::{OffsetBuffer, ScalarBuffer};
use arrow_extendr::IntoArrowRobj;
use extendr_api::prelude::*;
use geoarrow::array::{CoordBuffer, PolygonArray, SeparatedCoordBuffer};
use geoarrow::datatypes::{Crs, Dimension, Metadata};

use crate::aggregate::try_group_codes;
use crate::{check_pair_len, try_float_array};

/// Build polygons from coordinate columns
///
/// Returns one polygon per run of `polygon_id`, with one ring per run of
/// `linestring_id` inside it. `ga_polygon()` takes the first ring of each
/// polygon as its exterior and the rest as holes.
///
/// @details
/// `ga_polygon()` matches `sfheaders::sf_polygon()`, taking the columns
/// themselves rather than a data frame and their names. It reads the rows in
/// order, so you must keep the rows of each polygon together. `ga_polygon()`
/// errors when a `polygon_id` returns after a different one rather than
/// merging the two runs, and it orders the polygons by where each
/// `polygon_id` first appears. It starts a new ring at the start of each
/// polygon and wherever `linestring_id` changes, so you can number the rings
/// of every polygon from 1.
///
/// With `close = TRUE`, `ga_polygon()` repeats the first coordinate at the end
/// of any ring whose last coordinate differs from it in any dimension, and
/// errors on a ring with fewer than four coordinates once closed. When no ring
/// needs closing, it reuses the coordinate arrays without copying them.
///
/// `ga_polygon()` adds the z and m dimensions when you supply `z` and `m`. It
/// errors on `NA` in any coordinate or id. It stores `crs` verbatim in the
/// array's GeoArrow metadata and never interprets it. If you leave it `NULL`,
/// it produces an array with no CRS.
///
/// @param x,y numeric vectors or float64 arrays of coordinates, one row per
///   vertex
/// @param z,m numeric vectors or float64 arrays of z and m values, or `NULL`
///   to leave out that dimension
/// @param polygon_id a numeric vector, character vector, or Arrow array naming
///   the polygon of each row, or `NULL` for a single polygon
/// @param linestring_id a numeric vector, character vector, or Arrow array
///   naming the ring of each row within its polygon, or `NULL` for one ring
///   per polygon
/// @param close whether to close each open ring by repeating its first
///   coordinate
/// @param crs a coordinate reference system to record, or `NULL` for none
/// @returns a GeoArrow polygon array with one element per polygon
/// @export
/// @family construct
/// @examplesIf requireNamespace("geoarrow", quietly = TRUE) && requireNamespace("sf", quietly = TRUE)
/// # a square with a square hole, then a triangle
/// x <- c(0, 10, 10, 0, 2, 4, 4, 2, 20, 30, 20)
/// y <- c(0, 0, 10, 10, 2, 2, 4, 4, 0, 0, 10)
/// polygon_id <- c(1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2)
/// linestring_id <- c(1, 1, 1, 1, 2, 2, 2, 2, 1, 1, 1)
///
/// polys <- ga_polygon(x, y, polygon_id = polygon_id, linestring_id = linestring_id)
/// sf::st_as_sfc(geoarrow::as_geoarrow_vctr(polys))
///
/// # supplying z gives a POLYGON Z
/// sf::st_as_sfc(geoarrow::as_geoarrow_vctr(
///   ga_polygon(c(0, 1, 1), c(0, 0, 1), z = c(5, 5, 6))
/// ))
#[extendr]
#[allow(clippy::too_many_arguments)]
fn ga_polygon(
    x: Robj,
    y: Robj,
    #[extendr(default = "NULL")] z: Robj,
    #[extendr(default = "NULL")] m: Robj,
    #[extendr(default = "NULL")] polygon_id: Robj,
    #[extendr(default = "NULL")] linestring_id: Robj,
    #[extendr(default = "TRUE")] close: bool,
    #[extendr(default = "NULL")] crs: Option<String>,
) -> extendr_api::Result<Robj> {
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

    let n = columns[0].1.len();
    for (label, values) in &columns {
        check_pair_len(n, values.len(), "x", label)?;
        if values.null_count() > 0 {
            return Err(Error::Other(format!("`{label}` must not contain NA")));
        }
    }

    let vertices = Vertices {
        coords: columns.into_iter().map(|(_, values)| values).collect(),
        dim,
        polygons: Vertices::ids(polygon_id, "polygon_id", n)?,
        rings: Vertices::ids(linestring_id, "linestring_id", n)?,
    };

    let metadata = match crs {
        Some(value) => Arc::new(Metadata::new(Crs::from_unknown_crs_type(value), None)),
        None => Arc::new(Metadata::default()),
    };

    vertices.into_polygons(close, metadata)?.into_arrow_robj()
}

/// Polygon vertices as coordinate columns, with the polygon and ring code of each row.
struct Vertices {
    coords: Vec<Float64Array>,
    dim: Dimension,
    polygons: Vec<i32>,
    rings: Vec<i32>,
}

impl Vertices {
    /// Read an id column as one code per row, or the same code for every row when it is `NULL`.
    fn ids(robj: Robj, label: &'static str, n: usize) -> extendr_api::Result<Vec<i32>> {
        if robj.is_null() {
            return Ok(vec![0; n]);
        }
        let codes = try_group_codes(robj, label)?;
        check_pair_len(n, codes.len(), "x", label)?;
        codes
            .into_iter()
            .map(|code| code.ok_or_else(|| Error::Other(format!("`{label}` must not contain NA"))))
            .collect()
    }

    /// The rows of each ring, and how many rings each polygon has.
    fn rings(&self) -> extendr_api::Result<(Vec<Range<usize>>, Vec<usize>)> {
        let mut rings = Vec::<Range<usize>>::new();
        let mut counts = Vec::new();
        let mut seen = HashSet::new();

        for (row, (&polygon, &ring)) in self.polygons.iter().zip(&self.rings).enumerate() {
            let new_polygon = row == 0 || polygon != self.polygons[row - 1];
            if new_polygon {
                if !seen.insert(polygon) {
                    return Err(Error::Other(format!(
                        "`polygon_id` must keep the rows of each polygon together, but row {} returns to an earlier polygon",
                        row + 1
                    )));
                }
                counts.push(0);
            }

            if new_polygon || ring != self.rings[row - 1] {
                rings.push(row..row);
                if let Some(count) = counts.last_mut() {
                    *count += 1;
                }
            }

            if let Some(rows) = rings.last_mut() {
                rows.end = row + 1;
            }
        }

        Ok((rings, counts))
    }

    /// Assemble the polygon array, closing each open ring when `close` is true.
    fn into_polygons(
        self,
        close: bool,
        metadata: Arc<Metadata>,
    ) -> extendr_api::Result<PolygonArray> {
        let (rings, counts) = self.rings()?;

        let mut lengths = Vec::with_capacity(rings.len());
        let mut open = Vec::with_capacity(rings.len());
        let mut remaining = rings.iter();
        for (polygon, &count) in counts.iter().enumerate() {
            for (ring, rows) in remaining.by_ref().take(count).enumerate() {
                let is_open = close
                    && self
                        .coords
                        .iter()
                        .any(|c| c.value(rows.start) != c.value(rows.end - 1));
                let len = rows.len() + usize::from(is_open);
                if close && len < 4 {
                    return Err(Error::Other(format!(
                        "ring {} of polygon {} has {len} coordinates once closed, but a closed ring needs at least 4",
                        ring + 1,
                        polygon + 1
                    )));
                }
                lengths.push(len);
                open.push(is_open);
            }
        }

        // reuse the input buffers when no ring gains a closing coordinate
        let copy = open.contains(&true);
        let buffers = self
            .coords
            .iter()
            .map(|c| {
                if !copy {
                    return c.values().clone();
                }
                rings
                    .iter()
                    .zip(&open)
                    .flat_map(|(rows, &is_open)| rows.clone().chain(is_open.then_some(rows.start)))
                    .map(|row| c.value(row))
                    .collect::<ScalarBuffer<f64>>()
            })
            .collect::<Vec<_>>();

        let coords = SeparatedCoordBuffer::from_vec(buffers, self.dim)
            .map_err(|e| Error::Other(e.to_string()))?;
        PolygonArray::try_new(
            CoordBuffer::Separated(coords),
            OffsetBuffer::from_lengths(counts),
            OffsetBuffer::from_lengths(lengths),
            None,
            metadata,
        )
        .map_err(|e| Error::Other(e.to_string()))
    }
}

extendr_module! {
    mod polygon;
    fn ga_polygon;
}
