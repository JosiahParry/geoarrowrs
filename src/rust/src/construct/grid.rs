use arrow::array::Array;
use arrow::buffer::ScalarBuffer;
use arrow_extendr::IntoArrowRobj;
use extendr_api::prelude::*;
use geoarrow::datatypes::Dimension;
use geoarrow_cast::downcast::NativeType;

use super::CoordArgs;
use super::columns::Runs;
use crate::envelope::{as_rects, extent_of};
use crate::threads::Threads;
use crate::{as_geometry_chunks, try_float_array};

/// Build a grid of squares over an extent
///
/// Returns the cells of a square grid covering the bounding box of `x`, as
/// polygons, as the points at their centres, or as the points at their
/// corners. This matches `sf::st_make_grid()` for square grids.
///
/// @details
/// `ga_make_grid()` starts the grid at `offset`, the lower left corner of `x`
/// unless you give it, and steps `cellsize` across and up. Without `cellsize`,
/// it divides the extent of `x` into `n` cells each way, `n` being 10 by 10
/// unless you give it. Without `n`, it adds cells until the grid covers `x`.
/// With all of `cellsize`, `offset`, and `n`, it needs no `x`, and with none of
/// them and no `x` it returns the 10 degree grid over the globe that sf does.
///
/// `ga_make_grid()` orders the cells along x first, then up in y, and starts
/// each square at its lower left corner, as sf does. It records the CRS of `x`
/// unless you give `crs`.
///
/// `ga_make_grid()` builds only square grids. It has no hexagonal grid, which
/// `sf::st_make_grid(square = FALSE)` gives.
///
/// @param x a GeoArrow geometry array whose bounding box the grid covers, or
///   `NULL` when you give `cellsize`, `offset`, and `n`
/// @param cellsize the width and height of a cell; length 1 or 2
/// @param offset the x and y of the lower left corner of the grid; length 2
/// @param n the number of cells across and up; length 1 or 2
/// @param crs a coordinate reference system to record, or `NULL` for the CRS
///   of `x`
/// @param what one of `"polygons"`, `"centers"`, or `"corners"`
/// @returns a GeoArrow polygon array of the cells, or a point array of their
///   centres or corners
/// @export
/// @family construct
/// @examplesIf requireNamespace("sf", quietly = TRUE) && requireNamespace("geoarrow", quietly = TRUE)
/// nc <- as.data.frame(read_shapefile(
///   system.file("shape/nc.shp", package = "sf")
/// ))
///
/// # a 4 by 2 grid of cells over the state
/// cells <- ga_make_grid(nc$geometry, n = c(4, 2))
/// sf::st_as_sfc(geoarrow::as_geoarrow_vctr(cells))
///
/// # half degree cells, and the point at the centre of each
/// centres <- ga_make_grid(nc$geometry, cellsize = 0.5, what = "centers")
/// centres$length
#[extendr]
fn ga_make_grid(
    #[extendr(default = "NULL")] x: Robj,
    #[extendr(default = "NULL")] cellsize: Robj,
    #[extendr(default = "NULL")] offset: Robj,
    #[extendr(default = "NULL")] n: Robj,
    #[extendr(default = "NULL")] crs: Option<String>,
    #[extendr(default = "\"polygons\"")] what: &str,
) -> extendr_api::Result<Robj> {
    // a length 1 or 2 numeric argument as an x and y pair, or None when it is NULL
    let pair = |robj: Robj, label: &'static str, lengths: &[usize]| {
        if robj.is_null() {
            return Ok(None);
        }
        let values = try_float_array(robj, label)?;
        if !lengths.contains(&values.len()) || values.null_count() > 0 {
            return Err(Error::Other(format!(
                "`{label}` must be {} numbers with no NA",
                if lengths.len() == 1 { "2" } else { "1 or 2" }
            )));
        }
        Ok(Some([values.value(0), values.value(values.len() - 1)]))
    };
    let mut cellsize = pair(cellsize, "cellsize", &[1, 2])?;
    let mut offset = pair(offset, "offset", &[2])?;
    let mut n = pair(n, "n", &[1, 2])?;

    let mut metadata = CoordArgs::metadata(crs.clone());
    if x.is_null() && cellsize.is_none() && offset.is_none() && n.is_none() {
        // sf's grid when given nothing: 10 degree cells over the globe
        cellsize = Some([10.0, 10.0]);
        offset = Some([-180.0, -90.0]);
        n = Some([36.0, 18.0]);
        metadata = CoordArgs::metadata(crs.or(Some("EPSG:4326".to_string())));
    }

    let (bb, metadata) = match (cellsize, offset, n) {
        (Some(c), Some(o), Some(k)) if x.is_null() => (
            [o[0], o[1], o[0] + k[0] * c[0], o[1] + k[1] * c[1]],
            metadata,
        ),
        _ if x.is_null() => {
            return Err(Error::Other(
                "give `x`, or all of `cellsize`, `offset`, and `n`".to_string(),
            ));
        }
        _ => {
            let chunks = as_geometry_chunks(x)?;
            let metadata = if metadata.crs().crs_value().is_some() {
                metadata
            } else {
                chunks[0].data_type().metadata().clone()
            };
            let bb = extent_of(&as_rects(Threads::get(), &chunks)?)
                .map(|r| [r.min().x, r.min().y, r.max().x, r.max().y])
                .filter(|bb| bb.iter().all(|v| v.is_finite()));
            let Some(bb) = bb else {
                return Err(Error::Other("`x` has no extent to cover".to_string()));
            };
            (bb, metadata)
        }
    };

    let origin = offset.unwrap_or([bb[0], bb[1]]);
    let step = cellsize.unwrap_or_else(|| {
        let k = n.unwrap_or([10.0, 10.0]);
        [(bb[2] - bb[0]) / k[0], (bb[3] - bb[1]) / k[1]]
    });
    let counts = n.unwrap_or([
        ((bb[2] - origin[0]) / step[0]).ceil(),
        ((bb[3] - origin[1]) / step[1]).ceil(),
    ]);
    if counts
        .iter()
        .any(|k| !k.is_finite() || *k < 1.0 || k.fract() != 0.0)
    {
        return Err(Error::Other(format!(
            "the grid must have a whole number of at least one cell each way, got {} by {}",
            counts[0], counts[1]
        )));
    }
    let (nx, ny) = (counts[0] as usize, counts[1] as usize);

    // the cell edges along one axis, the way sf computes them
    let edges = |axis: usize, k: usize| {
        let from = origin[axis];
        if cellsize.is_some() {
            return (0..=k)
                .map(|i| from + i as f64 * step[axis])
                .collect::<Vec<_>>();
        }
        // R's seq(from, to, length.out = k + 1) ends exactly on `to`
        let to = bb[axis + 2];
        let by = (to - from) / k as f64;
        (0..=k)
            .map(|i| if i == k { to } else { from + i as f64 * by })
            .collect::<Vec<_>>()
    };
    let (xc, yc) = (edges(0, nx), edges(1, ny));

    let (xs, ys, kind) = match what {
        "polygons" => {
            let mut xs = Vec::with_capacity(nx * ny * 5);
            let mut ys = Vec::with_capacity(nx * ny * 5);
            for j in 0..ny {
                for i in 0..nx {
                    xs.extend([xc[i], xc[i + 1], xc[i + 1], xc[i], xc[i]]);
                    ys.extend([yc[j], yc[j], yc[j + 1], yc[j + 1], yc[j]]);
                }
            }
            (xs, ys, NativeType::Polygon)
        }
        "centers" => {
            let half = [0.5 * (xc[1] - xc[0]), 0.5 * (yc[1] - yc[0])];
            let (xs, ys) = yc[1..]
                .iter()
                .flat_map(|y| xc[1..].iter().map(move |x| (x - half[0], y - half[1])))
                .unzip();
            (xs, ys, NativeType::Point)
        }
        "corners" => {
            let (xs, ys) = yc
                .iter()
                .flat_map(|y| xc.iter().map(move |x| (*x, *y)))
                .unzip();
            (xs, ys, NativeType::Point)
        }
        other => {
            return Err(Error::Other(format!(
                "`what` must be one of \"polygons\", \"centers\", or \"corners\", got \"{other}\""
            )));
        }
    };

    // each square is one ring of five corners, and each point one row
    let size = if kind == NativeType::Polygon { 5 } else { 1 };
    let cells = xs.len() / size;
    let runs = Runs {
        coords: vec![ScalarBuffer::from(xs), ScalarBuffer::from(ys)],
        dim: Dimension::XY,
        metadata,
        ranges: (0..cells).map(|k| k * size..(k + 1) * size).collect(),
        counts: if kind == NativeType::Polygon {
            vec![vec![1; cells]]
        } else {
            Vec::new()
        },
    };
    runs.into_array(kind, false, None)?.into_arrow_robj()
}

extendr_module! {
    mod grid;
    fn ga_make_grid;
}
