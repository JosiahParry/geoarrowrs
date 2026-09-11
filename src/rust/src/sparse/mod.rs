use arrow::array::{Array, ListBuilder, UInt32Builder};
use arrow_extendr::IntoArrowRobj;
use extendr_api::prelude::*;
use geo::algorithm::contains::Contains;
use geo::algorithm::coordinate_position::{CoordPos, CoordinatePosition};
use geo::algorithm::relate::{IntersectionMatrix, Relate};
use geo::algorithm::winding_order::Winding;
use geo::indexed::{IntervalTreeMultiPolygon, PreparedGeometry};
use geo::{Coord, Geometry, MultiPolygon};
use geo_index::rtree::sort::HilbertSort;
use geo_index::rtree::{DEFAULT_RTREE_NODE_SIZE, RTree as GeoRTree, RTreeBuilder, RTreeIndex};
use rayon::prelude::*;
use std::borrow::Cow;
mod dwithin;
mod knn;
mod metric;
mod pairs;

use crate::envelope::rects_of;
use crate::threads::with_pool;
use crate::{as_geo_geometries_par, as_geometry_chunks};

/// One side of a sparse predicate: the geometries, and the boxes an index query needs.
type GeomsAndRects = (Vec<Option<Geometry<f64>>>, Vec<Option<geo::Rect<f64>>>);

/// Read a geometry argument as geometries alongside the boxes an index query needs.
fn as_geoms_and_rects(robj: Robj) -> extendr_api::Result<GeomsAndRects> {
    let geoms = as_geo_geometries_par(&as_geometry_chunks(robj)?)?;
    let rects = rects_of(&geoms);
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

/// Which DE-9IM relationship a sparse predicate is asking about.
#[derive(Clone, Copy, PartialEq)]
enum Predicate {
    Intersects,
    Contains,
    ContainsProperly,
    Within,
    Covers,
    CoveredBy,
    Touches,
    Crosses,
    Overlaps,
    EqualsTopo,
}

impl Predicate {
    fn test(self, m: &IntersectionMatrix) -> bool {
        match self {
            Self::Intersects => m.is_intersects(),
            Self::Contains => m.is_contains(),
            Self::ContainsProperly => m.is_contains_properly(),
            Self::Within => m.is_within(),
            Self::Covers => m.is_covers(),
            Self::CoveredBy => m.is_coveredby(),
            Self::Touches => m.is_touches(),
            Self::Crosses => m.is_crosses(),
            Self::Overlaps => m.is_overlaps(),
            Self::EqualsTopo => m.is_equal_topo(),
        }
    }

    /// The answer from where a point sits, when `y` is that point.
    ///
    /// A point has an empty boundary and a zero dimensional interior, so the
    /// whole matrix follows from whether it lies inside, on, or outside `x`,
    /// and no topology graph has to be built. `None` means there is no such
    /// shortcut and the matrix is needed.
    fn at_position(self, pos: CoordPos) -> Option<bool> {
        match self {
            Self::Intersects | Self::Covers => Some(pos != CoordPos::Outside),
            Self::Contains | Self::ContainsProperly => Some(pos == CoordPos::Inside),
            Self::Touches => Some(pos == CoordPos::OnBoundary),
            _ => None,
        }
    }

    /// Whether the shortcut is exactly "the point lies inside", which geo can answer from an index.
    fn is_inside_only(self) -> bool {
        matches!(self, Self::Contains | Self::ContainsProperly)
    }

    /// The same shortcut with the arguments swapped, so `x` is the point.
    fn at_position_swapped(self, pos: CoordPos) -> Option<bool> {
        match self {
            Self::Intersects | Self::CoveredBy => Some(pos != CoordPos::Outside),
            Self::Within => Some(pos == CoordPos::Inside),
            Self::Touches => Some(pos == CoordPos::OnBoundary),
            _ => None,
        }
    }
}

/// Candidates a row needs before indexing its edges pays for building the index.
const INDEX_MIN_CANDIDATES: usize = 16;

/// The point a geometry is, when it is a single point.
///
/// A collection is left out: its boundary follows the mod 2 rule, which is not
/// what the predicates below assume.
fn as_point(geom: &Geometry<f64>) -> Option<Coord<f64>> {
    match geom {
        Geometry::Point(p) => Some(p.0),
        _ => None,
    }
}

/// The surface a geometry is, borrowed when it is already a multipolygon.
fn as_multipolygon(geom: &Geometry<f64>) -> Option<Cow<'_, MultiPolygon<f64>>> {
    match geom {
        Geometry::MultiPolygon(mp) => Some(Cow::Borrowed(mp)),
        Geometry::Polygon(p) => Some(Cow::Owned(MultiPolygon::new(vec![p.clone()]))),
        _ => None,
    }
}

/// Whether every ring is wound the way the OGC asks, exteriors one way and holes the other.
///
/// The interval tree pools every ring into one winding number, so a hole wound
/// the same way as its exterior reads as inside it. Walking the rings one at a
/// time does not care, so the index may only stand in when the winding agrees
/// with what it assumes.
fn is_canonically_wound(mp: &MultiPolygon<f64>) -> bool {
    mp.iter()
        .all(|p| p.exterior().is_ccw() && p.interiors().iter().all(|hole| hole.is_cw()))
}

/// An interval tree over one row's edges, when it would earn its keep.
///
/// A ray cast only consults the edges whose vertical span covers the point, so
/// indexing them by that span turns a walk of every edge into a lookup. geo's
/// own tree is used, whose `contains` is defined as the point being inside, so
/// this stands in for exactly the predicates that ask only that.
fn edge_index(
    geom: &Geometry<f64>,
    predicate: Predicate,
    candidates: usize,
) -> Option<IntervalTreeMultiPolygon<f64>> {
    if !predicate.is_inside_only() || candidates < INDEX_MIN_CANDIDATES {
        return None;
    }
    as_multipolygon(geom)
        .filter(|mp| is_canonically_wound(mp))
        .map(|mp| IntervalTreeMultiPolygon::new(mp.as_ref()))
}

/// Whether every geometry that is present is a single point.
fn all_points(geoms: &[Option<Geometry<f64>>]) -> bool {
    geoms
        .iter()
        .flatten()
        .all(|g| matches!(g, Geometry::Point(_)))
}

/// Every sparse predicate is the same walk: narrow with the tree, confirm with DE-9IM.
fn sparse_predicate(x: Robj, y: Robj, predicate: Predicate) -> extendr_api::Result<Robj> {
    let (xs, x_rects) = as_geoms_and_rects(x)?;
    let (ys, y_rects) = as_geoms_and_rects(y)?;

    // a point on either side turns the matrix into a ray cast, which is the
    // difference between a graph per candidate pair and a walk of the edges
    let ys_are_points = all_points(&ys) && predicate.at_position(CoordPos::Inside).is_some();
    let xs_are_points =
        all_points(&xs) && predicate.at_position_swapped(CoordPos::Inside).is_some();

    let found = match index_rects(&y_rects) {
        Some((tree, positions)) => with_pool(|| {
            xs.par_iter()
                .zip(x_rects.par_iter())
                .map(|(geom, rect)| {
                    let (Some(geom), Some(rect)) = (geom, rect) else {
                        return None;
                    };
                    let candidates = tree
                        .search(rect.min().x, rect.min().y, rect.max().x, rect.max().y)
                        .into_iter()
                        .filter_map(|i| positions.get(i as usize).copied())
                        .collect::<Vec<_>>();

                    let mut rows = if candidates.is_empty() {
                        // most rows match nothing, and noding one is the expensive part
                        Vec::new()
                    } else if ys_are_points {
                        let index = edge_index(geom, predicate, candidates.len());
                        candidates
                            .into_iter()
                            .filter(|row| {
                                let Some(pt) = ys[*row as usize].as_ref().and_then(as_point) else {
                                    return false;
                                };
                                match &index {
                                    Some(index) => index.contains(&pt),
                                    None => predicate
                                        .at_position(geom.coordinate_position(&pt))
                                        .unwrap_or(false),
                                }
                            })
                            .map(|row| row + 1)
                            .collect::<Vec<_>>()
                    } else if xs_are_points {
                        let Some(pt) = as_point(geom) else {
                            return Some(Vec::new());
                        };
                        candidates
                            .into_iter()
                            .filter(|row| {
                                ys[*row as usize]
                                    .as_ref()
                                    .and_then(|other| {
                                        predicate
                                            .at_position_swapped(other.coordinate_position(&pt))
                                    })
                                    .unwrap_or(false)
                            })
                            .map(|row| row + 1)
                            .collect::<Vec<_>>()
                    } else {
                        let prepared = PreparedGeometry::from(geom);
                        candidates
                            .into_iter()
                            .filter(|row| {
                                ys[*row as usize]
                                    .as_ref()
                                    .is_some_and(|other| predicate.test(&prepared.relate(other)))
                            })
                            .map(|row| row + 1)
                            .collect::<Vec<_>>()
                    };
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

    rows_to_list_array(found)
}

/// Pack the matched rows as one list per row of `x`, a null where there was nothing to search with.
pub(super) fn rows_to_list_array(found: Vec<Option<Vec<u32>>>) -> extendr_api::Result<Robj> {
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
    sparse_predicate(x, y, Predicate::Intersects)
}

/// @export
/// @rdname sparse
/// @family topology
#[extendr]
fn ga_sparse_contains(x: Robj, y: Robj) -> extendr_api::Result<Robj> {
    sparse_predicate(x, y, Predicate::Contains)
}

/// @export
/// @rdname sparse
/// @family topology
#[extendr]
fn ga_sparse_contains_properly(x: Robj, y: Robj) -> extendr_api::Result<Robj> {
    sparse_predicate(x, y, Predicate::ContainsProperly)
}

/// @export
/// @rdname sparse
/// @family topology
#[extendr]
fn ga_sparse_within(x: Robj, y: Robj) -> extendr_api::Result<Robj> {
    sparse_predicate(x, y, Predicate::Within)
}

/// @export
/// @rdname sparse
/// @family topology
#[extendr]
fn ga_sparse_covers(x: Robj, y: Robj) -> extendr_api::Result<Robj> {
    sparse_predicate(x, y, Predicate::Covers)
}

/// @export
/// @rdname sparse
/// @family topology
#[extendr]
fn ga_sparse_covered_by(x: Robj, y: Robj) -> extendr_api::Result<Robj> {
    sparse_predicate(x, y, Predicate::CoveredBy)
}

/// @export
/// @rdname sparse
/// @family topology
#[extendr]
fn ga_sparse_touches(x: Robj, y: Robj) -> extendr_api::Result<Robj> {
    sparse_predicate(x, y, Predicate::Touches)
}

/// @export
/// @rdname sparse
/// @family topology
#[extendr]
fn ga_sparse_crosses(x: Robj, y: Robj) -> extendr_api::Result<Robj> {
    sparse_predicate(x, y, Predicate::Crosses)
}

/// @export
/// @rdname sparse
/// @family topology
#[extendr]
fn ga_sparse_overlaps(x: Robj, y: Robj) -> extendr_api::Result<Robj> {
    sparse_predicate(x, y, Predicate::Overlaps)
}

/// @export
/// @rdname sparse
/// @family topology
#[extendr]
fn ga_sparse_equals_topo(x: Robj, y: Robj) -> extendr_api::Result<Robj> {
    sparse_predicate(x, y, Predicate::EqualsTopo)
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
    use dwithin;
    use knn;
    use pairs;
}
