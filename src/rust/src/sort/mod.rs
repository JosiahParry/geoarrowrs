use arrow::array::{Array, Int32Array};
use arrow_extendr::IntoArrowRobj;
use extendr_api::prelude::*;

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
    let chunks = as_geometry_chunks(geometry)?;
    let rects = as_rects(Threads::get(), &chunks)?;

    let centres = rects
        .iter()
        .map(|r| r.map(|r| r.center()))
        .collect::<Vec<_>>();
    let (min_x, min_y, max_x, max_y) = centres.iter().flatten().fold(
        (
            f64::INFINITY,
            f64::INFINITY,
            f64::NEG_INFINITY,
            f64::NEG_INFINITY,
        ),
        |(x0, y0, x1, y1), c| (x0.min(c.x), y0.min(c.y), x1.max(c.x), y1.max(c.y)),
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

    let mut keyed = Vec::with_capacity(centres.len());
    let mut nulls = Vec::new();
    for (row, centre) in centres.iter().enumerate() {
        match centre {
            Some(c) => keyed.push((
                (cell(c.x, min_x, width), cell(c.y, min_y, height)).hilbert(),
                row,
            )),
            None => nulls.push(row),
        }
    }
    // a stable sort keeps the original order among rows in the same cell
    keyed.sort_by_key(|(h, _)| *h);

    let order = keyed
        .into_iter()
        .map(|(_, row)| row)
        .chain(nulls)
        .map(|row| row as i32 + 1)
        .collect::<Vec<_>>();

    Int32Array::from(order).into_data().into_arrow_robj()
}

extendr_module! {
    mod sort;
    fn ga_hilbert_order;
}
