use arrow::array::{Array, ListBuilder, UInt32Builder};
use arrow_extendr::IntoArrowRobj;
use extendr_api::prelude::*;

use geo_index::rtree::sort::{HilbertSort, STRSort};
use geo_index::rtree::{DEFAULT_RTREE_NODE_SIZE, RTree as GeoRTree, RTreeBuilder, RTreeIndex};
use geoarrow_array::GeoArrowArray;
mod kdtree;

use crate::as_geometry_chunks;
use crate::envelope::as_rects;

// Spatial index over a geometry array
#[extendr]
pub struct RTree {
    tree: GeoRTree<f64>,
    /// Rows whose geometry was null or empty are never added, so map tree positions back.
    positions: Vec<u32>,
    n: usize,
}

/// Turn the 0 based tree positions into the 1 based row indices R expects.
fn to_r_indices(positions: &[u32], found: Vec<u32>) -> anyhow::Result<Robj> {
    let mut bldr = UInt32Builder::with_capacity(found.len());
    let mut rows: Vec<u32> = found
        .into_iter()
        .filter_map(|i| positions.get(i as usize).copied())
        .map(|i| i + 1)
        .collect();
    rows.sort_unstable();
    for row in rows {
        bldr.append_value(row);
    }
    bldr.finish()
        .into_data()
        .into_arrow_robj()
        .map_err(|e| anyhow::anyhow!("{e}"))
}

/// A spatial index over a geometry array
///
/// Indexes the bounding box of each geometry so that queries can skip the rows
/// that cannot match.
///
/// @details
/// The tree is a packed Hilbert R-tree, the same structure FlatGeobuf stores
/// on disk. It is built once and is immutable, so it pays off when a set of
/// geometries is queried repeatedly.
///
/// Every query is a bounding box test, not an exact one. Two geometries whose
/// boxes overlap need not themselves intersect, so treat results as candidates
/// and confirm with [ga_intersects()] when exactness matters. Narrowing to
/// candidates first is the point: the exact test then runs on a handful of
/// rows rather than all of them.
///
/// Rows with a null or empty geometry have no bounding box and are left out of
/// the tree, so a query never returns them. Row numbers always refer to
/// positions in the original array.
///
/// @returns an `RTree` object
/// @export
/// @family index
/// @references [geo-index](https://docs.rs/geo-index/latest/geo_index/)
/// @examplesIf requireNamespace("sf", quietly = TRUE) && requireNamespace("geoarrow", quietly = TRUE)
/// nc <- as.data.frame(read_shapefile(
///   system.file("shape/nc.shp", package = "sf")
/// ))
/// idx <- RTree$new(nc$geometry)
///
/// idx$size()
///
/// # candidate rows whose bounding box meets the query box
/// hits <- as.vector(idx$search(-79, 35, -78, 36))
/// length(hits)
///
/// # the three rows nearest a point
/// as.vector(idx$neighbors(-79, 35, max_results = 3))
#[extendr]
impl RTree {
    /// Build the index. `node_size` sets how many entries share a tree node;
    /// larger values build faster and query slower. `sort` picks the packing
    /// order, either `"hilbert"` or `"str"`.
    fn new(
        geometry: Robj,
        #[extendr(default = "16")] node_size: i32,
        #[extendr(default = "\"hilbert\"")] sort: &str,
    ) -> anyhow::Result<Self> {
        if node_size < 2 {
            anyhow::bail!("`node_size` must be at least 2");
        }
        if !matches!(sort, "hilbert" | "str") {
            anyhow::bail!("`sort` must be either \"hilbert\" or \"str\", got \"{sort}\"");
        }

        let chunks = as_geometry_chunks(geometry).map_err(|e| anyhow::anyhow!("{e}"))?;
        let n: usize = chunks.iter().map(|c| c.len()).sum();
        let rects = as_rects(&chunks).map_err(|e| anyhow::anyhow!("{e}"))?;

        let mut boxes = Vec::with_capacity(n);
        let mut positions = Vec::with_capacity(n);
        for (row, rect) in rects.into_iter().enumerate() {
            if let Some(rect) = rect {
                boxes.push((rect.min().x, rect.min().y, rect.max().x, rect.max().y));
                positions.push(row as u32);
            }
        }

        if boxes.is_empty() {
            anyhow::bail!("Cannot build an index: no geometry has a bounding box");
        }

        let node_size = u16::try_from(node_size).unwrap_or(DEFAULT_RTREE_NODE_SIZE);
        let mut bldr = RTreeBuilder::<f64>::new_with_node_size(boxes.len() as u32, node_size);
        for (min_x, min_y, max_x, max_y) in boxes {
            bldr.add(min_x, min_y, max_x, max_y);
        }

        let tree = match sort {
            "str" => bldr.finish::<STRSort>(),
            _ => bldr.finish::<HilbertSort>(),
        };

        Ok(Self { tree, positions, n })
    }

    /// Find the rows whose bounding box overlaps each geometry
    ///
    /// Returns one list of candidate row numbers per element of `geometry`,
    /// so the result lines up row for row with the query array. This is the
    /// shape a spatial join needs.
    ///
    /// @details
    /// Anything with a bounding box works: a point array, a polygon array, or
    /// the box array [ga_envelope()] produces. Boxes are taken as they are and
    /// everything else is reduced to its envelope first.
    ///
    /// This is a bounding box test, not an exact one, so each list holds
    /// candidates to confirm with [ga_intersects()] or another predicate. A
    /// null or empty query geometry gives a null rather than an empty list.
    ///
    /// @param geometry a GeoArrow array to look up
    /// @returns a list array of 1 based row numbers, the same length as
    ///   `geometry`
    fn query(&self, geometry: Robj) -> anyhow::Result<Robj> {
        let chunks = as_geometry_chunks(geometry).map_err(|e| anyhow::anyhow!("{e}"))?;
        let rects = as_rects(&chunks).map_err(|e| anyhow::anyhow!("{e}"))?;

        let mut bldr = ListBuilder::new(UInt32Builder::new());
        for rect in rects {
            match rect {
                Some(rect) => {
                    let found =
                        self.tree
                            .search(rect.min().x, rect.min().y, rect.max().x, rect.max().y);
                    let mut rows = found
                        .into_iter()
                        .filter_map(|i| self.positions.get(i as usize).copied())
                        .map(|i| i + 1)
                        .collect::<Vec<_>>();
                    rows.sort_unstable();
                    for row in rows {
                        bldr.values().append_value(row);
                    }
                    bldr.append(true);
                }
                None => bldr.append(false),
            }
        }

        bldr.finish()
            .into_data()
            .into_arrow_robj()
            .map_err(|e| anyhow::anyhow!("{e}"))
    }

    /// Find the rows whose bounding box overlaps a query box
    ///
    /// Returns the row numbers whose bounding box intersects the given box,
    /// in increasing order.
    ///
    /// @details
    /// This is a bounding box test, not an exact one. Two geometries whose
    /// boxes overlap need not themselves intersect, so treat the result as a
    /// set of candidates and confirm with [ga_intersects()] when exactness
    /// matters.
    ///
    /// @param xmin,ymin,xmax,ymax the query box
    /// @returns an integer array of 1 based row numbers
    fn search(&self, xmin: f64, ymin: f64, xmax: f64, ymax: f64) -> anyhow::Result<Robj> {
        to_r_indices(&self.positions, self.tree.search(xmin, ymin, xmax, ymax))
    }

    /// Find the rows nearest a point
    ///
    /// Returns row numbers ordered by how close their bounding box is to the
    /// point.
    ///
    /// @details
    /// Distance is measured to the bounding box rather than to the geometry
    /// itself, so this too gives candidates. `max_results` caps how many come
    /// back and `max_distance` caps how far the search goes; either can be
    /// `NULL`.
    ///
    /// @param x,y the query point
    /// @param max_results the most rows to return, or `NULL` for no limit
    /// @param max_distance the furthest to search, or `NULL` for no limit
    /// @returns an integer array of 1 based row numbers
    fn neighbors(
        &self,
        x: f64,
        y: f64,
        #[extendr(default = "NULL")] max_results: Nullable<i32>,
        #[extendr(default = "NULL")] max_distance: Nullable<f64>,
    ) -> anyhow::Result<Robj> {
        let max_results = match max_results {
            Nullable::NotNull(k) if k < 1 => {
                anyhow::bail!("`max_results` must be at least 1");
            }
            Nullable::NotNull(k) => Some(k as usize),
            Nullable::Null => None,
        };
        let max_distance = match max_distance {
            Nullable::NotNull(d) => Some(d),
            Nullable::Null => None,
        };

        let found = self.tree.neighbors(x, y, max_results, max_distance);
        let mut bldr = UInt32Builder::with_capacity(found.len());
        for i in found {
            if let Some(row) = self.positions.get(i as usize) {
                bldr.append_value(row + 1);
            }
        }
        bldr.finish()
            .into_data()
            .into_arrow_robj()
            .map_err(|e| anyhow::anyhow!("{e}"))
    }

    /// The number of rows the index was built over
    ///
    /// @returns the length of the array the index was built from
    fn size(&self) -> i32 {
        self.n as i32
    }

    /// The number of rows actually held in the tree
    ///
    /// @details
    /// Lower than `size()` when the array held null or empty geometries, which
    /// have no bounding box to index.
    ///
    /// @returns the number of indexed rows
    fn n_indexed(&self) -> i32 {
        self.positions.len() as i32
    }
}

extendr_module! {
    mod index;
    impl RTree;
    use kdtree;
}
