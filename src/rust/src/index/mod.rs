use arrow::array::{Array, UInt32Builder};
use arrow_extendr::IntoArrowRobj;
use extendr_api::prelude::*;

use geo::algorithm::bounding_rect::BoundingRect;
use geo_index::rtree::sort::HilbertSort;
use geo_index::rtree::{DEFAULT_RTREE_NODE_SIZE, RTree as GeoRTree, RTreeBuilder, RTreeIndex};
use geoarrow_array::GeoArrowArray;

use crate::{as_geo_geometries, as_geometry_chunks};

/// A packed Hilbert R-tree over the bounding boxes of a geometry array.
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
/// and confirm with [intersects()] when exactness matters. Narrowing to
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
/// hits <- as.vector(nanoarrow::convert_array(idx$search(-79, 35, -78, 36)))
/// length(hits)
///
/// # the three rows nearest a point
/// as.vector(nanoarrow::convert_array(idx$neighbors(-79, 35, max_results = 3)))
#[extendr]
impl RTree {
    /// Build the index. `node_size` sets how many entries share a tree node;
    /// larger values build faster and query slower.
    fn new(geometry: Robj, #[extendr(default = "16")] node_size: i32) -> anyhow::Result<Self> {
        if node_size < 2 {
            anyhow::bail!("`node_size` must be at least 2");
        }

        let chunks = as_geometry_chunks(geometry).map_err(|e| anyhow::anyhow!("{e}"))?;
        let n: usize = chunks.iter().map(|c| c.len()).sum();

        let mut boxes: Vec<(f64, f64, f64, f64)> = Vec::with_capacity(n);
        let mut positions: Vec<u32> = Vec::with_capacity(n);
        let mut row = 0u32;
        for chunk in &chunks {
            let geoms = as_geo_geometries(chunk.as_ref()).map_err(|e| anyhow::anyhow!("{e}"))?;
            for geom in geoms {
                if let Some(rect) = geom.as_ref().and_then(|g| g.bounding_rect()) {
                    boxes.push((rect.min().x, rect.min().y, rect.max().x, rect.max().y));
                    positions.push(row);
                }
                row += 1;
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

        Ok(Self {
            tree: bldr.finish::<HilbertSort>(),
            positions,
            n,
        })
    }

    /// Find the rows whose bounding box overlaps a query box
    ///
    /// Returns the row numbers whose bounding box intersects the given box,
    /// in increasing order.
    ///
    /// @details
    /// This is a bounding box test, not an exact one. Two geometries whose
    /// boxes overlap need not themselves intersect, so treat the result as a
    /// set of candidates and confirm with [intersects()] when exactness
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
}
