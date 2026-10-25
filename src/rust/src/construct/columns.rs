use std::collections::HashSet;
use std::ops::Range;
use std::sync::Arc;

use arrow::array::Array;
use arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_extendr::IntoArrowRobj;
use extendr_api::prelude::*;
use geoarrow::array::{
    CoordBuffer, GeoArrowArray, LineStringArray, MultiLineStringArray, MultiPointArray,
    MultiPolygonArray, PointArray, PolygonArray, SeparatedCoordBuffer,
};
use geoarrow::datatypes::{Dimension, Metadata};
use geoarrow_cast::downcast::NativeType;

use super::CoordArgs;
use crate::aggregate::try_group_codes;
use crate::check_pair_len;

/// Build linestrings from coordinate columns
///
/// Returns one linestring per run of `linestring_id`, through its rows in
/// order.
///
/// @details
/// `ga_linestring()` matches `sfheaders::sf_linestring()`, taking the columns
/// themselves rather than a data frame and their names. It reads the rows in
/// order, so you must keep the rows of each linestring together:
/// `ga_linestring()` errors when a `linestring_id` returns after a different
/// one, and orders the linestrings by where each id first appears.
///
/// `ga_linestring()` adds the z and m dimensions when you supply `z` and `m`.
/// It errors on `NA` in any coordinate or id, and reuses the coordinate arrays
/// without copying them.
///
/// @inheritParams ga_polygon
/// @param linestring_id a numeric vector, character vector, or Arrow array
///   naming the linestring of each row, or `NULL` for a single linestring
/// @returns a GeoArrow linestring array with one element per linestring
/// @export
/// @family construct
/// @examplesIf requireNamespace("geoarrow", quietly = TRUE) && requireNamespace("sf", quietly = TRUE)
/// lines <- ga_linestring(
///   c(0, 1, 2, 10, 11),
///   c(0, 1, 0, 5, 5),
///   linestring_id = c(1, 1, 1, 2, 2)
/// )
/// sf::st_as_sfc(geoarrow::as_geoarrow_vctr(lines))
#[extendr]
fn ga_linestring(
    x: Robj,
    y: Robj,
    #[extendr(default = "NULL")] z: Robj,
    #[extendr(default = "NULL")] m: Robj,
    #[extendr(default = "NULL")] linestring_id: Robj,
    #[extendr(default = "NULL")] crs: Option<String>,
) -> extendr_api::Result<Robj> {
    Runs::from_columns(x, y, z, m, crs, vec![(linestring_id, "linestring_id")])?
        .into_array(NativeType::LineString, false, None)?
        .into_arrow_robj()
}

/// Build multipoints from coordinate columns
///
/// Returns one multipoint per run of `multipoint_id`, holding its rows in
/// order.
///
/// @details
/// `ga_multipoint()` matches `sfheaders::sf_multipoint()`, taking the columns
/// themselves rather than a data frame and their names. You must keep the rows
/// of each multipoint together: `ga_multipoint()` errors when a
/// `multipoint_id` returns after a different one, and orders the multipoints
/// by where each id first appears.
///
/// `ga_multipoint()` adds the z and m dimensions when you supply `z` and `m`.
/// It errors on `NA` in any coordinate or id, and reuses the coordinate arrays
/// without copying them.
///
/// @inheritParams ga_polygon
/// @param multipoint_id a numeric vector, character vector, or Arrow array
///   naming the multipoint of each row, or `NULL` for a single multipoint
/// @returns a GeoArrow multipoint array with one element per multipoint
/// @export
/// @family construct
/// @examplesIf requireNamespace("geoarrow", quietly = TRUE) && requireNamespace("sf", quietly = TRUE)
/// pts <- ga_multipoint(c(0, 1, 5), c(0, 1, 5), multipoint_id = c("a", "a", "b"))
/// sf::st_as_sfc(geoarrow::as_geoarrow_vctr(pts))
#[extendr]
fn ga_multipoint(
    x: Robj,
    y: Robj,
    #[extendr(default = "NULL")] z: Robj,
    #[extendr(default = "NULL")] m: Robj,
    #[extendr(default = "NULL")] multipoint_id: Robj,
    #[extendr(default = "NULL")] crs: Option<String>,
) -> extendr_api::Result<Robj> {
    Runs::from_columns(x, y, z, m, crs, vec![(multipoint_id, "multipoint_id")])?
        .into_array(NativeType::MultiPoint, false, None)?
        .into_arrow_robj()
}

/// Build multilinestrings from coordinate columns
///
/// Returns one multilinestring per run of `multilinestring_id`, with one line
/// per run of `linestring_id` inside it.
///
/// @details
/// `ga_multilinestring()` matches `sfheaders::sf_multilinestring()`, taking the
/// columns themselves rather than a data frame and their names. You must keep
/// the rows of each multilinestring together: `ga_multilinestring()` errors
/// when a `multilinestring_id` returns after a different one, and orders the
/// multilinestrings by where each id first appears. It starts a new line at
/// the start of each multilinestring and wherever `linestring_id` changes, so
/// you can number the lines of every multilinestring from 1.
///
/// `ga_multilinestring()` adds the z and m dimensions when you supply `z` and
/// `m`. It errors on `NA` in any coordinate or id, and reuses the coordinate
/// arrays without copying them.
///
/// @inheritParams ga_polygon
/// @param multilinestring_id a numeric vector, character vector, or Arrow
///   array naming the multilinestring of each row, or `NULL` for a single
///   multilinestring
/// @param linestring_id a numeric vector, character vector, or Arrow array
///   naming the line of each row within its multilinestring, or `NULL` for one
///   line per multilinestring
/// @returns a GeoArrow multilinestring array with one element per
///   multilinestring
/// @export
/// @family construct
/// @examplesIf requireNamespace("geoarrow", quietly = TRUE) && requireNamespace("sf", quietly = TRUE)
/// lines <- ga_multilinestring(
///   c(0, 1, 5, 6, 10, 11),
///   c(0, 1, 5, 6, 0, 1),
///   multilinestring_id = c(1, 1, 1, 1, 2, 2),
///   linestring_id = c(1, 1, 2, 2, 1, 1)
/// )
/// sf::st_as_sfc(geoarrow::as_geoarrow_vctr(lines))
#[extendr]
fn ga_multilinestring(
    x: Robj,
    y: Robj,
    #[extendr(default = "NULL")] z: Robj,
    #[extendr(default = "NULL")] m: Robj,
    #[extendr(default = "NULL")] multilinestring_id: Robj,
    #[extendr(default = "NULL")] linestring_id: Robj,
    #[extendr(default = "NULL")] crs: Option<String>,
) -> extendr_api::Result<Robj> {
    let ids = vec![
        (multilinestring_id, "multilinestring_id"),
        (linestring_id, "linestring_id"),
    ];
    Runs::from_columns(x, y, z, m, crs, ids)?
        .into_array(NativeType::MultiLineString, false, None)?
        .into_arrow_robj()
}

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
    let ids = vec![(polygon_id, "polygon_id"), (linestring_id, "linestring_id")];
    let runs = Runs::from_columns(x, y, z, m, crs, ids)?;
    runs.check_rings(close)?;
    runs.into_array(NativeType::Polygon, close, None)?
        .into_arrow_robj()
}

/// Build multipolygons from coordinate columns
///
/// Returns one multipolygon per run of `multipolygon_id`, with one polygon per
/// run of `polygon_id` inside it and one ring per run of `linestring_id`
/// inside each polygon.
///
/// @details
/// `ga_multipolygon()` matches `sfheaders::sf_multipolygon()`, taking the
/// columns themselves rather than a data frame and their names. You must keep
/// the rows of each multipolygon together: `ga_multipolygon()` errors when a
/// `multipolygon_id` returns after a different one, and orders the
/// multipolygons by where each id first appears. It starts a new polygon at
/// the start of each multipolygon and wherever `polygon_id` changes, and a new
/// ring at the start of each polygon and wherever `linestring_id` changes. It
/// takes the first ring of each polygon as its exterior and the rest as holes.
///
/// `ga_multipolygon()` closes rings the way [ga_polygon()] does, and adds the z
/// and m dimensions when you supply `z` and `m`. It errors on `NA` in any
/// coordinate or id.
///
/// @inheritParams ga_polygon
/// @param multipolygon_id a numeric vector, character vector, or Arrow array
///   naming the multipolygon of each row, or `NULL` for a single multipolygon
/// @param polygon_id a numeric vector, character vector, or Arrow array naming
///   the polygon of each row within its multipolygon, or `NULL` for one polygon
///   per multipolygon
/// @returns a GeoArrow multipolygon array with one element per multipolygon
/// @export
/// @family construct
/// @examplesIf requireNamespace("geoarrow", quietly = TRUE) && requireNamespace("sf", quietly = TRUE)
/// # two squares in one multipolygon, then a triangle on its own
/// polys <- ga_multipolygon(
///   c(0, 1, 1, 0, 5, 6, 6, 5, 20, 30, 20),
///   c(0, 0, 1, 1, 5, 5, 6, 6, 0, 0, 10),
///   multipolygon_id = c(1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2),
///   polygon_id = c(1, 1, 1, 1, 2, 2, 2, 2, 1, 1, 1)
/// )
/// sf::st_as_sfc(geoarrow::as_geoarrow_vctr(polys))
#[extendr]
#[allow(clippy::too_many_arguments)]
fn ga_multipolygon(
    x: Robj,
    y: Robj,
    #[extendr(default = "NULL")] z: Robj,
    #[extendr(default = "NULL")] m: Robj,
    #[extendr(default = "NULL")] multipolygon_id: Robj,
    #[extendr(default = "NULL")] polygon_id: Robj,
    #[extendr(default = "NULL")] linestring_id: Robj,
    #[extendr(default = "TRUE")] close: bool,
    #[extendr(default = "NULL")] crs: Option<String>,
) -> extendr_api::Result<Robj> {
    let ids = vec![
        (multipolygon_id, "multipolygon_id"),
        (polygon_id, "polygon_id"),
        (linestring_id, "linestring_id"),
    ];
    let runs = Runs::from_columns(x, y, z, m, crs, ids)?;
    runs.check_rings(close)?;
    runs.into_array(NativeType::MultiPolygon, close, None)?
        .into_arrow_robj()
}

/// Coordinate columns cut into nested parts: the row range of each innermost part, and the part count of each level above it, outermost first.
pub(super) struct Runs {
    pub(super) coords: Vec<ScalarBuffer<f64>>,
    pub(super) dim: Dimension,
    pub(super) metadata: Arc<Metadata>,
    pub(super) ranges: Vec<Range<usize>>,
    pub(super) counts: Vec<Vec<usize>>,
}

impl Runs {
    /// Read the coordinate and id arguments of a column constructor, outermost id first, and cut the rows into parts.
    fn from_columns(
        x: Robj,
        y: Robj,
        z: Robj,
        m: Robj,
        crs: Option<String>,
        ids: Vec<(Robj, &'static str)>,
    ) -> extendr_api::Result<Self> {
        let CoordArgs {
            columns,
            dim,
            metadata,
        } = CoordArgs::read(x, y, z, m, crs)?;

        let n = columns[0].1.len();
        for (label, values) in &columns {
            check_pair_len(n, values.len(), "x", label)?;
            if values.null_count() > 0 {
                return Err(Error::Other(format!("`{label}` must not contain NA")));
            }
        }

        let codes = ids
            .into_iter()
            .map(|(robj, label)| Ok((label, Self::ids(robj, label, n)?)))
            .collect::<extendr_api::Result<Vec<_>>>()?;
        let coords = columns
            .into_iter()
            .map(|(_, values)| values.values().clone())
            .collect();
        Self::from_ids(coords, dim, metadata, &codes)
    }

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

    /// Start a part at each level wherever its id or an outer id changes, erroring when an outermost id returns.
    fn from_ids(
        coords: Vec<ScalarBuffer<f64>>,
        dim: Dimension,
        metadata: Arc<Metadata>,
        ids: &[(&'static str, Vec<i32>)],
    ) -> extendr_api::Result<Self> {
        let levels = ids.len();
        let n = ids.first().map_or(0, |(_, codes)| codes.len());
        let mut ranges = Vec::<Range<usize>>::new();
        let mut counts = vec![Vec::<usize>::new(); levels.saturating_sub(1)];
        let mut seen = HashSet::new();

        for row in 0..n {
            let changed = (0..levels).find(|&k| row == 0 || ids[k].1[row] != ids[k].1[row - 1]);
            if let Some(start) = changed {
                if start == 0 && !seen.insert(ids[0].1[row]) {
                    return Err(Error::Other(format!(
                        "`{}` must keep the rows of each id together, but row {} returns to an earlier id",
                        ids[0].0,
                        row + 1
                    )));
                }
                for level in start..levels {
                    if let Some(parent) = level.checked_sub(1).and_then(|p| counts[p].last_mut()) {
                        *parent += 1;
                    }
                    if level + 1 < levels {
                        counts[level].push(0);
                    } else {
                        ranges.push(row..row);
                    }
                }
            }
            if let Some(rows) = ranges.last_mut() {
                rows.end = row + 1;
            }
        }

        Ok(Self {
            coords,
            dim,
            metadata,
            ranges,
            counts,
        })
    }

    /// Append a coordinate buffer to the columns, returning the row it starts at.
    pub(super) fn append(
        columns: &mut [Vec<f64>],
        coords: &CoordBuffer,
    ) -> extendr_api::Result<usize> {
        let CoordBuffer::Separated(coords) = coords else {
            return Err(Error::Other("expected separated coordinates".to_string()));
        };
        let base = columns.first().map_or(0, |column| column.len());
        for (column, buffer) in columns.iter_mut().zip(coords.raw_buffers()) {
            column.extend_from_slice(&buffer[..coords.len()]);
        }
        Ok(base)
    }

    /// How many coordinates a part has once closed, when `close` is true.
    pub(super) fn closed_len(&self, rows: &Range<usize>, close: bool) -> usize {
        let open = close
            && !rows.is_empty()
            && self.coords.iter().any(|c| c[rows.start] != c[rows.end - 1]);
        rows.len() + usize::from(open)
    }

    /// Error on the first ring with fewer than four coordinates once closed, when `close` is true.
    fn check_rings(&self, close: bool) -> extendr_api::Result<()> {
        if !close {
            return Ok(());
        }
        match self
            .ranges
            .iter()
            .find(|rows| self.closed_len(rows, close) < 4)
        {
            Some(rows) => Err(Error::Other(format!(
                "the ring starting at row {} has {} coordinates once closed, but a closed ring needs at least 4",
                rows.start + 1,
                self.closed_len(rows, close)
            ))),
            None => Ok(()),
        }
    }

    /// Assemble the parts into an array of `kind`, closing each ring of a polygon kind when `close` is true.
    pub(super) fn into_array(
        self,
        kind: NativeType,
        close: bool,
        nulls: Option<NullBuffer>,
    ) -> extendr_api::Result<Arc<dyn GeoArrowArray>> {
        let close = close && matches!(kind, NativeType::Polygon | NativeType::MultiPolygon);
        let lengths = self
            .ranges
            .iter()
            .map(|rows| self.closed_len(rows, close))
            .collect::<Vec<_>>();

        // reuse the input buffers when the parts cover them in order with nothing to close
        let mut next = 0;
        let in_order = self.ranges.iter().zip(&lengths).all(|(rows, &len)| {
            let follows = len == rows.len() && rows.start == next;
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
                self.ranges
                    .iter()
                    .zip(&lengths)
                    .flat_map(|(rows, &len)| {
                        rows.clone().chain((len > rows.len()).then_some(rows.start))
                    })
                    .map(|row| c[row])
                    .collect::<ScalarBuffer<f64>>()
            })
            .collect::<Vec<_>>();
        let coords = CoordBuffer::Separated(
            SeparatedCoordBuffer::from_vec(buffers, self.dim)
                .map_err(|e| Error::Other(e.to_string()))?,
        );

        let offsets =
            |lengths: &[usize]| OffsetBuffer::<i32>::from_lengths(lengths.iter().copied());
        let metadata = self.metadata;
        let array = match (kind, self.counts.as_slice()) {
            (NativeType::Point, []) => PointArray::try_new(coords, nulls, metadata)
                .map(|a| Arc::new(a) as Arc<dyn GeoArrowArray>),
            (NativeType::LineString, []) => {
                LineStringArray::try_new(coords, offsets(&lengths), nulls, metadata)
                    .map(|a| Arc::new(a) as Arc<dyn GeoArrowArray>)
            }
            (NativeType::MultiPoint, []) => {
                MultiPointArray::try_new(coords, offsets(&lengths), nulls, metadata)
                    .map(|a| Arc::new(a) as Arc<dyn GeoArrowArray>)
            }
            (NativeType::Polygon, [rings]) => {
                PolygonArray::try_new(coords, offsets(rings), offsets(&lengths), nulls, metadata)
                    .map(|a| Arc::new(a) as Arc<dyn GeoArrowArray>)
            }
            (NativeType::MultiLineString, [lines]) => MultiLineStringArray::try_new(
                coords,
                offsets(lines),
                offsets(&lengths),
                nulls,
                metadata,
            )
            .map(|a| Arc::new(a) as Arc<dyn GeoArrowArray>),
            (NativeType::MultiPolygon, [polygons, rings]) => MultiPolygonArray::try_new(
                coords,
                offsets(polygons),
                offsets(rings),
                offsets(&lengths),
                nulls,
                metadata,
            )
            .map(|a| Arc::new(a) as Arc<dyn GeoArrowArray>),
            _ => {
                return Err(Error::Other(
                    "the ids do not nest into this geometry type".to_string(),
                ));
            }
        };
        array.map_err(|e| Error::Other(e.to_string()))
    }
}

extendr_module! {
    mod columns;
    fn ga_linestring;
    fn ga_multipoint;
    fn ga_multilinestring;
    fn ga_polygon;
    fn ga_multipolygon;
}
