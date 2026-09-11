use arrow::array::{Array, ListBuilder, UInt32Builder};
use arrow_extendr::IntoArrowRobj;
use extendr_api::prelude::*;
use geo::Geometry;
use geo::algorithm::relate::{IntersectionMatrix, Relate};
use geo_index::rtree::sort::HilbertSort;
use geo_index::rtree::{DEFAULT_RTREE_NODE_SIZE, RTree as GeoRTree, RTreeBuilder, RTreeIndex};
use rayon::prelude::*;

use crate::envelope::rect_of;
use crate::threads::with_pool;
use crate::{as_geo_geometries, as_geometry_chunks};

/// One side of a sparse predicate: the geometries, and the boxes an index query needs.
type GeomsAndRects = (Vec<Option<Geometry<f64>>>, Vec<Option<geo::Rect<f64>>>);

/// Read a geometry argument as geometries alongside the boxes an index query needs.
fn as_geoms_and_rects(robj: Robj) -> extendr_api::Result<GeomsAndRects> {
    let chunks = as_geometry_chunks(robj)?;
    let mut geoms = Vec::new();
    for chunk in &chunks {
        geoms.extend(as_geo_geometries(chunk.as_ref())?);
    }
    let rects = geoms.iter().map(|g| rect_of(g.as_ref())).collect();
    Ok((geoms, rects))
}

/// Index the boxes that exist, keeping the row each one came from.
fn index_rects(rects: &[Option<geo::Rect<f64>>]) -> Option<(GeoRTree<f64>, Vec<u32>)> {
    let mut boxes = Vec::with_capacity(rects.len());
    let mut positions = Vec::with_capacity(rects.len());
    for (row, rect) in rects.iter().enumerate() {
        if let Some(rect) = rect {
            boxes.push(*rect);
            positions.push(row as u32);
        }
    }

    if boxes.is_empty() {
        return None;
    }

    let mut bldr =
        RTreeBuilder::<f64>::new_with_node_size(boxes.len() as u32, DEFAULT_RTREE_NODE_SIZE);
    for rect in boxes {
        bldr.add(rect.min().x, rect.min().y, rect.max().x, rect.max().y);
    }

    Some((bldr.finish::<HilbertSort>(), positions))
}

/// Every sparse predicate is the same walk: narrow with the tree, confirm with DE-9IM.
fn sparse_predicate(
    x: Robj,
    y: Robj,
    test: fn(&IntersectionMatrix) -> bool,
) -> extendr_api::Result<Robj> {
    let (xs, x_rects) = as_geoms_and_rects(x)?;
    let (ys, y_rects) = as_geoms_and_rects(y)?;

    let found = match index_rects(&y_rects) {
        Some((tree, positions)) => with_pool(|| {
            xs.par_iter()
                .zip(x_rects.par_iter())
                .map(|(geom, rect)| {
                    let (Some(geom), Some(rect)) = (geom, rect) else {
                        return None;
                    };
                    let mut rows = tree
                        .search(rect.min().x, rect.min().y, rect.max().x, rect.max().y)
                        .into_iter()
                        .filter_map(|i| positions.get(i as usize).copied())
                        .filter(|row| {
                            ys[*row as usize]
                                .as_ref()
                                .is_some_and(|other| test(&geom.relate(other)))
                        })
                        .map(|row| row + 1)
                        .collect::<Vec<_>>();
                    rows.sort_unstable();
                    Some(rows)
                })
                .collect::<Vec<_>>()
        }),
        // no row of `y` has a box, so nothing matches, but the shape still has to line up
        None => x_rects
            .iter()
            .map(|rect| rect.as_ref().map(|_| Vec::new()))
            .collect(),
    };

    let mut bldr = ListBuilder::new(UInt32Builder::new());
    for rows in found {
        match rows {
            Some(rows) => {
                for row in rows {
                    bldr.values().append_value(row);
                }
                bldr.append(true);
            }
            None => bldr.append(false),
        }
    }

    bldr.finish().into_data().into_arrow_robj()
}

/// Find which rows of `y` relate to each row of `x`
///
/// Each function compares every row of `x` against every row of `y` and returns
/// the row numbers of `y` for which the named DE-9IM relationship holds. This is
/// the sparse form of the pairwise predicates in [ga_intersects()], and the
/// shape [ga_join()] needs.
///
/// @details
/// The comparison is not the full cross product. `y` is indexed in a packed
/// Hilbert R-tree and only the rows whose bounding box overlaps are relate
/// tested, so the cost scales with the number of candidates rather than with
/// `length(x) * length(y)`.
///
/// A row of `x` that matches nothing gives a zero length element, not a null. A
/// null or empty geometry in `x` gives a null element, and one in `y` is never
/// returned.
///
/// [ga_disjoint()] has no sparse form. Disjointness is the one relationship a
/// bounding box cannot narrow, so the answer is almost every row of `y` and the
/// result is denser than the input.
///
/// @param x a GeoArrow geometry array
/// @param y a GeoArrow geometry array
/// @returns a list array of 1 based row numbers into `y`, the same length as `x`
/// @export
/// @rdname sparse
/// @family topology
/// @references [Relate](https://docs.rs/geo/latest/geo/algorithm/relate/trait.Relate.html)
/// @examplesIf requireNamespace("sf", quietly = TRUE) && requireNamespace("geoarrow", quietly = TRUE)
/// nc <- as.data.frame(read_shapefile(
///   system.file("shape/nc.shp", package = "sf")
/// ))
///
/// # which counties each county touches
/// nbrs <- as.vector(ga_sparse_touches(nc$geometry, nc$geometry))
/// nbrs[[1]]
///
/// # how many neighbours each has
/// summary(lengths(nbrs))
#[extendr]
fn ga_sparse_intersects(x: Robj, y: Robj) -> extendr_api::Result<Robj> {
    sparse_predicate(x, y, IntersectionMatrix::is_intersects)
}

/// @export
/// @rdname sparse
/// @family topology
#[extendr]
fn ga_sparse_contains(x: Robj, y: Robj) -> extendr_api::Result<Robj> {
    sparse_predicate(x, y, IntersectionMatrix::is_contains)
}

/// @export
/// @rdname sparse
/// @family topology
#[extendr]
fn ga_sparse_contains_properly(x: Robj, y: Robj) -> extendr_api::Result<Robj> {
    sparse_predicate(x, y, IntersectionMatrix::is_contains_properly)
}

/// @export
/// @rdname sparse
/// @family topology
#[extendr]
fn ga_sparse_within(x: Robj, y: Robj) -> extendr_api::Result<Robj> {
    sparse_predicate(x, y, IntersectionMatrix::is_within)
}

/// @export
/// @rdname sparse
/// @family topology
#[extendr]
fn ga_sparse_covers(x: Robj, y: Robj) -> extendr_api::Result<Robj> {
    sparse_predicate(x, y, IntersectionMatrix::is_covers)
}

/// @export
/// @rdname sparse
/// @family topology
#[extendr]
fn ga_sparse_covered_by(x: Robj, y: Robj) -> extendr_api::Result<Robj> {
    sparse_predicate(x, y, IntersectionMatrix::is_coveredby)
}

/// @export
/// @rdname sparse
/// @family topology
#[extendr]
fn ga_sparse_touches(x: Robj, y: Robj) -> extendr_api::Result<Robj> {
    sparse_predicate(x, y, IntersectionMatrix::is_touches)
}

/// @export
/// @rdname sparse
/// @family topology
#[extendr]
fn ga_sparse_crosses(x: Robj, y: Robj) -> extendr_api::Result<Robj> {
    sparse_predicate(x, y, IntersectionMatrix::is_crosses)
}

/// @export
/// @rdname sparse
/// @family topology
#[extendr]
fn ga_sparse_overlaps(x: Robj, y: Robj) -> extendr_api::Result<Robj> {
    sparse_predicate(x, y, IntersectionMatrix::is_overlaps)
}

/// @export
/// @rdname sparse
/// @family topology
#[extendr]
fn ga_sparse_equals_topo(x: Robj, y: Robj) -> extendr_api::Result<Robj> {
    sparse_predicate(x, y, IntersectionMatrix::is_equal_topo)
}

extendr_module! {
    mod sparse;
    fn ga_sparse_intersects;
    fn ga_sparse_contains;
    fn ga_sparse_contains_properly;
    fn ga_sparse_within;
    fn ga_sparse_covers;
    fn ga_sparse_covered_by;
    fn ga_sparse_touches;
    fn ga_sparse_crosses;
    fn ga_sparse_overlaps;
    fn ga_sparse_equals_topo;
}
