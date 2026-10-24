use arrow::array::{Array, Int32Array};
use arrow_extendr::IntoArrowRobj;
use extendr_api::prelude::*;
use geo::Coord;

use crate::as_geometry_chunks;
use crate::envelope::as_rects;
use crate::threads::Threads;

/// The largest cell coordinate on the 2^16 by 2^16 grid the curve runs through.
const HILBERT_MAX: f64 = 65535.0;

/// Position along a Hilbert curve through a 2^16 by 2^16 grid of cells.
trait Hilbert {
    fn hilbert(self) -> u32;
}

/// The bit twiddling curve flatbush packs its trees with, from "Fast Hilbert curve generation".
impl Hilbert for (u32, u32) {
    fn hilbert(self) -> u32 {
        let (x, y) = self;

        let mut a = x ^ y;
        let mut b = 0xFFFF ^ a;
        let mut c = 0xFFFF ^ (x | y);
        let mut d = x & (y ^ 0xFFFF);

        let mut aa = a | (b >> 1);
        let mut bb = (a >> 1) ^ a;
        let mut cc = ((c >> 1) ^ (b & (d >> 1))) ^ c;
        let mut dd = ((a & (c >> 1)) ^ (d >> 1)) ^ d;

        a = aa;
        b = bb;
        c = cc;
        d = dd;
        aa = (a & (a >> 2)) ^ (b & (b >> 2));
        bb = (a & (b >> 2)) ^ (b & ((a ^ b) >> 2));
        cc ^= (a & (c >> 2)) ^ (b & (d >> 2));
        dd ^= (b & (c >> 2)) ^ ((a ^ b) & (d >> 2));

        a = aa;
        b = bb;
        c = cc;
        d = dd;
        aa = (a & (a >> 4)) ^ (b & (b >> 4));
        bb = (a & (b >> 4)) ^ (b & ((a ^ b) >> 4));
        cc ^= (a & (c >> 4)) ^ (b & (d >> 4));
        dd ^= (b & (c >> 4)) ^ ((a ^ b) & (d >> 4));

        a = aa;
        b = bb;
        c = cc;
        d = dd;
        cc ^= (a & (c >> 8)) ^ (b & (d >> 8));
        dd ^= (b & (c >> 8)) ^ ((a ^ b) & (d >> 8));

        a = cc ^ (cc >> 1);
        b = dd ^ (dd >> 1);

        let mut i0 = x ^ y;
        let mut i1 = b | (0xFFFF ^ (i0 | a));

        i0 = (i0 | (i0 << 8)) & 0x00FF00FF;
        i0 = (i0 | (i0 << 4)) & 0x0F0F0F0F;
        i0 = (i0 | (i0 << 2)) & 0x33333333;
        i0 = (i0 | (i0 << 1)) & 0x55555555;

        i1 = (i1 | (i1 << 8)) & 0x00FF00FF;
        i1 = (i1 | (i1 << 4)) & 0x0F0F0F0F;
        i1 = (i1 | (i1 << 2)) & 0x33333333;
        i1 = (i1 | (i1 << 1)) & 0x55555555;

        (i1 << 1) | i0
    }
}

/// Order rows along a Hilbert curve
///
/// Returns the row order that puts geometries whose bounding box centres lie
/// close together next to each other. Pass it to `[` to sort an array or a
/// data frame so that nearby rows sit together.
///
/// @details
/// `ga_hilbert_order()` maps each centre onto a 65536 by 65536 grid over the
/// extent of every row and orders the rows by their position along a Hilbert
/// curve through that grid, the same curve [RTree] packs its tree with. If you
/// sort a table this way before you write GeoParquet, each row group covers a
/// small area, so a reader can skip the row groups that cannot match a query.
///
/// `ga_hilbert_order()` breaks ties by the original row order, and puts rows
/// with a null or empty geometry last, in their original order.
///
/// @param geometry a GeoArrow geometry array
/// @returns an integer array of 1 based row numbers, the same length as
///   `geometry`
/// @export
/// @family index
/// @examplesIf requireNamespace("sf", quietly = TRUE) && requireNamespace("geoarrow", quietly = TRUE)
/// nc <- as.data.frame(read_shapefile(
///   system.file("shape/nc.shp", package = "sf")
/// ))
/// ord <- as.vector(ga_hilbert_order(nc$geometry))
/// head(ord)
///
/// # neighbouring counties now sit next to each other
/// head(nc[ord, "NAME"])
#[extendr]
fn ga_hilbert_order(geometry: Robj) -> extendr_api::Result<Robj> {
    Centres::of(geometry)?.hilbert_order()
}

/// Order rows by sort-tile-recursive packing
///
/// Returns the row order that sort-tile-recursive (STR) packing gives the
/// bounding box centres of `geometry`. Pass it to `[` to sort an array or a
/// data frame so that nearby rows sit together.
///
/// @details
/// `ga_str_order()` sorts the rows by the x of their centres, cuts them into
/// vertical slices of `node_size` times the square root of the number of
/// leaves, and sorts each slice by y. That is the leaf order an STR packed
/// R-tree with `node_size` entries per node uses, the same packing
/// `RTree$new(sort = "str")` builds.
///
/// `ga_str_order()` breaks ties by the original row order, and puts rows with
/// a null or empty geometry last, in their original order.
///
/// @param geometry a GeoArrow geometry array
/// @param node_size how many rows share a leaf
/// @returns an integer array of 1 based row numbers, the same length as
///   `geometry`
/// @export
/// @family index
/// @examplesIf requireNamespace("sf", quietly = TRUE) && requireNamespace("geoarrow", quietly = TRUE)
/// nc <- as.data.frame(read_shapefile(
///   system.file("shape/nc.shp", package = "sf")
/// ))
/// ord <- as.vector(ga_str_order(nc$geometry, node_size = 4))
///
/// # the first slice runs south to north along the west of the state
/// head(nc[ord, "NAME"])
#[extendr]
fn ga_str_order(
    geometry: Robj,
    #[extendr(default = "16")] node_size: i32,
) -> extendr_api::Result<Robj> {
    if node_size < 2 {
        return Err(Error::Other("`node_size` must be at least 2".to_string()));
    }
    Centres::of(geometry)?.str_order(node_size as usize)
}

/// The bounding box centre of every row that has one, and the rows that do not.
struct Centres {
    points: Vec<(usize, Coord<f64>)>,
    nulls: Vec<usize>,
}

impl Centres {
    /// Read the centre of each row's bounding box, setting aside null and empty rows.
    fn of(geometry: Robj) -> extendr_api::Result<Self> {
        let chunks = as_geometry_chunks(geometry)?;
        let rects = as_rects(Threads::get(), &chunks)?;

        let mut points = Vec::with_capacity(rects.len());
        let mut nulls = Vec::new();
        for (row, rect) in rects.into_iter().enumerate() {
            match rect {
                Some(r) => points.push((row, r.center())),
                None => nulls.push(row),
            }
        }
        Ok(Self { points, nulls })
    }

    /// Rows along a Hilbert curve through the extent of every centre.
    fn hilbert_order(self) -> extendr_api::Result<Robj> {
        let (min_x, min_y, max_x, max_y) = self.points.iter().fold(
            (
                f64::INFINITY,
                f64::INFINITY,
                f64::NEG_INFINITY,
                f64::NEG_INFINITY,
            ),
            |(x0, y0, x1, y1), (_, c)| (x0.min(c.x), y0.min(c.y), x1.max(c.x), y1.max(c.y)),
        );
        let (width, height) = (max_x - min_x, max_y - min_y);

        // a zero span maps every centre to the first cell rather than dividing by zero
        let cell = |v: f64, lo: f64, span: f64| {
            if span > 0.0 {
                (HILBERT_MAX * (v - lo) / span).floor() as u32
            } else {
                0
            }
        };

        let mut keyed = self
            .points
            .iter()
            .map(|(row, c)| {
                let h = (cell(c.x, min_x, width), cell(c.y, min_y, height)).hilbert();
                (h, *row)
            })
            .collect::<Vec<_>>();
        // a stable sort keeps the original order among rows in the same cell
        keyed.sort_by_key(|(h, _)| *h);

        Self::finish(keyed.into_iter().map(|(_, row)| row), self.nulls)
    }

    /// Rows in sort-tile-recursive leaf order for `node_size` entries per leaf.
    fn str_order(mut self, node_size: usize) -> extendr_api::Result<Robj> {
        let leaves = self.points.len().div_ceil(node_size);
        let slices = (leaves as f64).sqrt().ceil() as usize;
        let per_slice = node_size * slices.max(1);

        // both sorts are stable, so ties keep the original row order
        self.points.sort_by(|a, b| a.1.x.total_cmp(&b.1.x));
        for slice in self.points.chunks_mut(per_slice) {
            slice.sort_by(|a, b| a.1.y.total_cmp(&b.1.y));
        }

        Self::finish(self.points.into_iter().map(|(row, _)| row), self.nulls)
    }

    /// 1 based rows in the given order with the null rows last, as an integer array.
    fn finish(
        ordered: impl Iterator<Item = usize>,
        nulls: Vec<usize>,
    ) -> extendr_api::Result<Robj> {
        let order = ordered
            .chain(nulls)
            .map(|row| row as i32 + 1)
            .collect::<Vec<_>>();
        Int32Array::from(order).into_data().into_arrow_robj()
    }
}

extendr_module! {
    mod sort;
    fn ga_hilbert_order;
    fn ga_str_order;
}
