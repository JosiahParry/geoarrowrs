use arrow::buffer::{NullBuffer, ScalarBuffer};
use arrow_extendr::IntoArrowRobj;
use extendr_api::prelude::*;
use geoarrow::array::GeoArrowArray;
use geoarrow::datatypes::CoordType;
use geoarrow_cast::downcast::NativeType;

use super::columns::Runs;
use crate::{as_linestring_chunks, as_multilinestring_chunks, check_pair_len};

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
        let base = Runs::append(&mut columns, chunk.coords())?;
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
            let base = Runs::append(&mut columns, chunk.coords())?;
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

    let mut runs = Runs {
        coords: columns.into_iter().map(ScalarBuffer::from).collect(),
        dim,
        metadata,
        ranges: Vec::new(),
        counts: vec![Vec::with_capacity(n)],
    };
    let mut valid = Vec::with_capacity(n);
    for (exterior, holes) in exteriors.into_iter().zip(interiors) {
        // an empty shell gives an empty polygon, so it adds no rings
        let rings = exterior
            .clone()
            .filter(|rows| !rows.is_empty())
            .map(|rows| std::iter::once(rows).chain(holes.into_iter().filter(|h| !h.is_empty())))
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        // a null shell or a ring too short to close has no answer
        let answered = exterior.is_some()
            && (!close || rings.iter().all(|rows| runs.closed_len(rows, close) >= 4));
        valid.push(answered);
        if let Some(counts) = runs.counts.first_mut() {
            counts.push(if answered { rings.len() } else { 0 });
        }
        if answered {
            runs.ranges.extend(rings);
        }
    }

    let nulls = valid.contains(&false).then(|| NullBuffer::from(valid));
    runs.into_array(NativeType::Polygon, close, nulls)?
        .into_arrow_robj()
}

extendr_module! {
    mod polygon;
    fn ga_make_polygon;
}
