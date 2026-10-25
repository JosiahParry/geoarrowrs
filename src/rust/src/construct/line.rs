use arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_extendr::IntoArrowRobj;
use extendr_api::prelude::*;
use geoarrow::array::{CoordBuffer, GeoArrowArray, LineStringArray, SeparatedCoordBuffer};
use geoarrow::datatypes::CoordType;

use crate::aggregate::{group_by_codes, try_group_codes};
use crate::as_point_chunks;

/// Join the points of each group into a line
///
/// Returns one linestring through the points that share each label of `by`,
/// taking the points in row order. With `by = NULL`, `ga_make_line_agg()`
/// joins every point into a single line.
///
/// @details
/// `ga_make_line_agg()` groups the rows the way [ga_collect_agg()] does. It
/// accepts an unsorted `by`, treats `NA` as a label like any other, and orders
/// the lines by where each label first appears, so you can name the lines with
/// `unique(by)`.
///
/// `ga_make_line_agg()` skips null and empty points, and returns a null line
/// for a group with fewer than two points left. It keeps the z and m values
/// and the CRS of `points`. This is the aggregate form of PostGIS
/// `ST_MakeLine()`; use [ga_make_line()] to join pairs of points instead.
///
/// @param points a GeoArrow point array
/// @param by a character vector, numeric vector, or Arrow array with one label
///   per point, or `NULL` for a single line
/// @returns a GeoArrow linestring array with one element per label of `by`
/// @export
/// @family construct
/// @examplesIf requireNamespace("geoarrow", quietly = TRUE) && requireNamespace("sf", quietly = TRUE)
/// pts <- ga_point(c(0, 1, 2, 10, 11), c(0, 1, 0, 5, 5))
/// lines <- ga_make_line_agg(pts, by = c("a", "a", "a", "b", "b"))
/// sf::st_as_sfc(geoarrow::as_geoarrow_vctr(lines))
///
/// as.vector(ga_length_euclidean(lines))
#[extendr]
fn ga_make_line_agg(
    points: Robj,
    #[extendr(default = "NULL")] by: Robj,
) -> extendr_api::Result<Robj> {
    let chunks = as_point_chunks(points)?
        .into_iter()
        .map(|chunk| chunk.into_coord_type(CoordType::Separated))
        .collect::<Vec<_>>();
    let Some(first) = chunks.first() else {
        return Err(Error::Other("`points` has no chunks".to_string()));
    };
    let dim = first.coords().dim();
    let metadata = first.data_type().metadata().clone();

    // one column per dimension across every chunk, and whether each point has coordinates
    let mut columns = vec![Vec::<f64>::new(); dim.size()];
    let mut usable = Vec::new();
    for chunk in &chunks {
        let CoordBuffer::Separated(coords) = chunk.coords() else {
            return Err(Error::Other("expected separated coordinates".to_string()));
        };
        let buffers = coords.raw_buffers();
        for (column, buffer) in columns.iter_mut().zip(buffers) {
            column.extend_from_slice(&buffer[..chunk.len()]);
        }
        usable.extend(
            (0..chunk.len()).map(|i| {
                !chunk.is_null(i) && buffers[0][i].is_finite() && buffers[1][i].is_finite()
            }),
        );
    }

    let n = usable.len();
    let groups = if by.is_null() {
        vec![(0..n).collect::<Vec<_>>()]
    } else {
        let rows = (0..n).map(Some).collect::<Vec<_>>();
        group_by_codes(rows, try_group_codes(by, "by")?, "points")?
    };

    let mut coords = vec![Vec::<f64>::new(); dim.size()];
    let mut lengths = Vec::with_capacity(groups.len());
    let mut valid = Vec::with_capacity(groups.len());
    for group in groups {
        let rows = group
            .into_iter()
            .filter(|&row| usable[row])
            .collect::<Vec<_>>();
        // a line needs two points, so a shorter group has no answer
        let is_line = rows.len() >= 2;
        if is_line {
            for (out, column) in coords.iter_mut().zip(&columns) {
                out.extend(rows.iter().map(|&row| column[row]));
            }
        }
        lengths.push(if is_line { rows.len() } else { 0 });
        valid.push(is_line);
    }

    let buffers = coords
        .into_iter()
        .map(ScalarBuffer::from)
        .collect::<Vec<_>>();
    let coords =
        SeparatedCoordBuffer::from_vec(buffers, dim).map_err(|e| Error::Other(e.to_string()))?;
    let nulls = valid.contains(&false).then(|| NullBuffer::from(valid));

    LineStringArray::try_new(
        CoordBuffer::Separated(coords),
        OffsetBuffer::from_lengths(lengths),
        nulls,
        metadata,
    )
    .map_err(|e| Error::Other(e.to_string()))?
    .into_arrow_robj()
}

extendr_module! {
    mod line;
    fn ga_make_line_agg;
}
