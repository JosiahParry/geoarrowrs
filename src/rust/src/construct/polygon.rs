use std::collections::HashSet;
use std::ops::Range;
use std::sync::Arc;

use arrow::array::Array;
use arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_extendr::IntoArrowRobj;
use extendr_api::prelude::*;
use geoarrow::array::{CoordBuffer, GeoArrowArray, PolygonArray, SeparatedCoordBuffer};
use geoarrow::datatypes::{CoordType, Crs, Dimension, Metadata};

use crate::aggregate::try_group_codes;
use crate::{as_linestring_chunks, as_multilinestring_chunks, check_pair_len, try_float_array};

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

    let coords = columns
        .into_iter()
        .map(|(_, values)| values.values().clone())
        .collect();
    let rings = Rings::from_ids(
        coords,
        dim,
        &Rings::ids(polygon_id, "polygon_id", n)?,
        &Rings::ids(linestring_id, "linestring_id", n)?,
    )?;
    if let Some((polygon, ring, len)) = rings.short_rings(close).first() {
        return Err(Error::Other(format!(
            "ring {} of polygon {} has {len} coordinates once closed, but a closed ring needs at least 4",
            ring + 1,
            polygon + 1
        )));
    }

    let metadata = match crs {
        Some(value) => Arc::new(Metadata::new(Crs::from_unknown_crs_type(value), None)),
        None => Arc::new(Metadata::default()),
    };

    rings.into_polygons(close, metadata)?.into_arrow_robj()
}

/// Build polygons from rings
///
/// Returns one polygon per row of `shell`, with `shell` as its exterior ring
/// and the lines on the same row of `holes` as its holes. This matches PostGIS
/// `ST_MakePolygon()`.
///
/// @details
/// With `close = TRUE`, `ga_make_polygon()` repeats the first coordinate at
/// the end of any ring whose last coordinate differs from it, as [ga_polygon()]
/// does. It returns a null polygon for a null shell and for a row with a ring
/// of fewer than four coordinates once closed, so the result always has the
/// same length as `shell`. It returns an empty polygon for an empty shell, and
/// skips null and empty holes.
///
/// `ga_make_polygon()` keeps the z and m values and the CRS of `shell`. Use
/// [ga_make_line_agg()] to join points into the rings first.
///
/// @param shell a GeoArrow linestring array of exterior rings
/// @param holes a GeoArrow multilinestring array the same length as `shell`,
///   holding the holes of each polygon, or `NULL` for none
/// @param close whether to close each open ring by repeating its first
///   coordinate
/// @returns a GeoArrow polygon array the same length as `shell`
/// @export
/// @family construct
/// @examplesIf requireNamespace("geoarrow", quietly = TRUE) && requireNamespace("sf", quietly = TRUE)
/// # join points into rings, then rings into polygons
/// pts <- ga_point(c(0, 10, 10, 0, 20, 30, 20), c(0, 0, 10, 10, 0, 0, 10))
/// rings <- ga_make_line_agg(pts, by = c(1, 1, 1, 1, 2, 2, 2))
/// sf::st_as_sfc(geoarrow::as_geoarrow_vctr(ga_make_polygon(rings)))
///
/// # a square with a square hole
/// shell <- ga_make_line_agg(ga_point(c(0, 10, 10, 0), c(0, 0, 10, 10)))
/// hole <- ga_make_line_agg(ga_point(c(2, 4, 4, 2), c(2, 2, 4, 4)))
/// sf::st_as_sfc(geoarrow::as_geoarrow_vctr(
///   ga_make_polygon(shell, ga_as_multilinestring(hole))
/// ))
#[extendr]
fn ga_make_polygon(
    shell: Robj,
    #[extendr(default = "NULL")] holes: Robj,
    #[extendr(default = "TRUE")] close: bool,
) -> extendr_api::Result<Robj> {
    let shells = as_linestring_chunks(shell)?
        .into_iter()
        .map(|chunk| chunk.into_coord_type(CoordType::Separated))
        .collect::<Vec<_>>();
    let Some(first) = shells.first() else {
        return Err(Error::Other("`shell` has no chunks".to_string()));
    };
    let dim = first.coords().dim();
    let metadata = first.data_type().metadata().clone();

    let mut columns = vec![Vec::<f64>::new(); dim.size()];
    let mut exteriors = Vec::new();
    for chunk in &shells {
        let base = Rings::append(&mut columns, chunk.coords())?;
        let offsets = chunk.geom_offsets();
        exteriors.extend((0..chunk.len()).map(|i| {
            (!chunk.is_null(i)).then(|| base + offsets[i] as usize..base + offsets[i + 1] as usize)
        }));
    }

    let n = exteriors.len();
    let mut interiors = vec![Vec::new(); n];
    if !holes.is_null() {
        let mut row = 0;
        for chunk in as_multilinestring_chunks(holes)? {
            let chunk = chunk.into_coord_type(CoordType::Separated);
            if chunk.coords().dim() != dim {
                return Err(Error::Other(
                    "`holes` must have the same dimensions as `shell`".to_string(),
                ));
            }
            let base = Rings::append(&mut columns, chunk.coords())?;
            let (lines, coords) = (chunk.geom_offsets(), chunk.ring_offsets());
            for i in 0..chunk.len() {
                if row < n && !chunk.is_null(i) {
                    interiors[row] = (lines[i] as usize..lines[i + 1] as usize)
                        .map(|line| base + coords[line] as usize..base + coords[line + 1] as usize)
                        .collect();
                }
                row += 1;
            }
        }
        check_pair_len(n, row, "shell", "holes")?;
    }

    let mut ranges = Vec::new();
    let mut counts = Vec::with_capacity(n);
    let mut valid = Vec::with_capacity(n);
    for (exterior, holes) in exteriors.into_iter().zip(interiors) {
        valid.push(exterior.is_some());
        // an empty shell gives an empty polygon, so it adds no rings
        let rings = exterior
            .filter(|rows| !rows.is_empty())
            .map(|rows| std::iter::once(rows).chain(holes.into_iter().filter(|h| !h.is_empty())))
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        counts.push(rings.len());
        ranges.extend(rings);
    }

    let coords = columns.into_iter().map(ScalarBuffer::from).collect();
    let mut rings = Rings {
        coords,
        dim,
        ranges,
        counts,
        valid,
    };
    for (polygon, _, _) in rings.short_rings(close) {
        rings.valid[polygon] = false;
    }

    rings.into_polygons(close, metadata)?.into_arrow_robj()
}

/// Rings as row ranges over coordinate columns, with how many rings each polygon has and whether it is valid.
struct Rings {
    coords: Vec<ScalarBuffer<f64>>,
    dim: Dimension,
    ranges: Vec<Range<usize>>,
    counts: Vec<usize>,
    valid: Vec<bool>,
}

impl Rings {
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

    /// Split rows into a polygon per run of `polygons` and a ring per run of `rings` within it.
    fn from_ids(
        coords: Vec<ScalarBuffer<f64>>,
        dim: Dimension,
        polygons: &[i32],
        rings: &[i32],
    ) -> extendr_api::Result<Self> {
        let mut ranges = Vec::<Range<usize>>::new();
        let mut counts = Vec::new();
        let mut seen = HashSet::new();

        for (row, (&polygon, &ring)) in polygons.iter().zip(rings).enumerate() {
            let new_polygon = row == 0 || polygon != polygons[row - 1];
            if new_polygon {
                if !seen.insert(polygon) {
                    return Err(Error::Other(format!(
                        "`polygon_id` must keep the rows of each polygon together, but row {} returns to an earlier polygon",
                        row + 1
                    )));
                }
                counts.push(0);
            }

            if new_polygon || ring != rings[row - 1] {
                ranges.push(row..row);
                if let Some(count) = counts.last_mut() {
                    *count += 1;
                }
            }

            if let Some(rows) = ranges.last_mut() {
                rows.end = row + 1;
            }
        }

        let valid = vec![true; counts.len()];
        Ok(Self {
            coords,
            dim,
            ranges,
            counts,
            valid,
        })
    }

    /// Append a coordinate buffer to the columns, returning the row it starts at.
    fn append(columns: &mut [Vec<f64>], coords: &CoordBuffer) -> extendr_api::Result<usize> {
        let CoordBuffer::Separated(coords) = coords else {
            return Err(Error::Other("expected separated coordinates".to_string()));
        };
        let base = columns.first().map_or(0, |column| column.len());
        for (column, buffer) in columns.iter_mut().zip(coords.raw_buffers()) {
            column.extend_from_slice(&buffer[..coords.len()]);
        }
        Ok(base)
    }

    /// How many coordinates a ring has once closed, when `close` is true.
    fn closed_len(&self, rows: &Range<usize>, close: bool) -> usize {
        let open = close
            && !rows.is_empty()
            && self.coords.iter().any(|c| c[rows.start] != c[rows.end - 1]);
        rows.len() + usize::from(open)
    }

    /// Each ring of a valid polygon too short to close, as its polygon, its place in that polygon, and its length once closed.
    fn short_rings(&self, close: bool) -> Vec<(usize, usize, usize)> {
        let mut short = Vec::new();
        if !close {
            return short;
        }
        let mut ranges = self.ranges.iter();
        for (polygon, (&count, &valid)) in self.counts.iter().zip(&self.valid).enumerate() {
            for (ring, rows) in ranges.by_ref().take(count).enumerate() {
                let len = self.closed_len(rows, close);
                if valid && len < 4 {
                    short.push((polygon, ring, len));
                }
            }
        }
        short
    }

    /// Assemble the polygon array, closing each open ring when `close` is true and leaving invalid polygons null.
    fn into_polygons(
        self,
        close: bool,
        metadata: Arc<Metadata>,
    ) -> extendr_api::Result<PolygonArray> {
        let mut counts = Vec::with_capacity(self.counts.len());
        let mut lengths = Vec::with_capacity(self.ranges.len());
        let mut kept = Vec::with_capacity(self.ranges.len());
        let mut ranges = self.ranges.iter();
        for (&count, &valid) in self.counts.iter().zip(&self.valid) {
            counts.push(if valid { count } else { 0 });
            for rows in ranges.by_ref().take(count) {
                if valid {
                    let len = self.closed_len(rows, close);
                    lengths.push(len);
                    kept.push((rows.clone(), len > rows.len()));
                }
            }
        }

        // reuse the input buffers when the kept rings cover them in order with nothing to close
        let mut next = 0;
        let in_order = kept.iter().all(|(rows, open)| {
            let follows = !open && rows.start == next;
            next = rows.end;
            follows
        });
        let as_given = in_order && next == self.coords.first().map_or(0, |c| c.len());

        let buffers = self
            .coords
            .iter()
            .map(|c| {
                if as_given {
                    return c.clone();
                }
                kept.iter()
                    .flat_map(|(rows, open)| rows.clone().chain(open.then_some(rows.start)))
                    .map(|row| c[row])
                    .collect::<ScalarBuffer<f64>>()
            })
            .collect::<Vec<_>>();

        let coords = SeparatedCoordBuffer::from_vec(buffers, self.dim)
            .map_err(|e| Error::Other(e.to_string()))?;
        let nulls = self
            .valid
            .contains(&false)
            .then(|| NullBuffer::from(self.valid));
        PolygonArray::try_new(
            CoordBuffer::Separated(coords),
            OffsetBuffer::from_lengths(counts),
            OffsetBuffer::from_lengths(lengths),
            nulls,
            metadata,
        )
        .map_err(|e| Error::Other(e.to_string()))
    }
}

extendr_module! {
    mod polygon;
    fn ga_polygon;
    fn ga_make_polygon;
}
