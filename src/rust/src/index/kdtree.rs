use arrow::array::{Array, ListBuilder, UInt32Builder};
use arrow_extendr::IntoArrowRobj;
use extendr_api::prelude::*;

use geo_index::kdtree::{
    DEFAULT_KDTREE_NODE_SIZE, KDTree as GeoKDTree, KDTreeBuilder, KDTreeIndex,
};
use geo_traits::to_geo::ToGeoPoint;
use geoarrow::array::{GeoArrowArray, GeoArrowArrayAccessor, PointArray};

use crate::envelope::as_rects;
use crate::{as_geometry_chunks, as_point_chunks, check_recycle_len, try_float_array};

// A k-d tree over the points of a geometry array
#[extendr]
pub struct KDTree {
    tree: GeoKDTree<f64>,
    /// Rows whose point was null or empty are never added, so map tree positions back.
    positions: Vec<u32>,
    n: usize,
}

/// Add every point of a GeoArrow point array to a tree, returning it with the row each point came from.
fn add_points(chunks: &[PointArray], node_size: u16) -> anyhow::Result<(GeoKDTree<f64>, Vec<u32>)> {
    let n = chunks.iter().map(|c| c.len()).sum();
    let mut coords = Vec::with_capacity(n);
    let mut positions = Vec::with_capacity(n);
    let mut row = 0u32;

    for chunk in chunks {
        for item in chunk.iter() {
            if let Some(Ok(point)) = item {
                let point = point.to_point();
                // an empty point carries NaN, which geo-index does not accept
                if point.x().is_finite() && point.y().is_finite() {
                    coords.push((point.x(), point.y()));
                    positions.push(row);
                }
            }
            row += 1;
        }
    }

    if coords.is_empty() {
        anyhow::bail!("Cannot build an index: the array holds no finite points");
    }

    let mut bldr = KDTreeBuilder::<f64>::new_with_node_size(coords.len() as u32, node_size);
    for (x, y) in coords {
        bldr.add(x, y);
    }

    Ok((bldr.finish(), positions))
}

/// A k-d tree over a point array
///
/// Finds which points sit in a box, or near another point.
///
/// @returns a `KDTree` object
/// @export
/// @family index
/// @references [geo-index](https://docs.rs/geo-index/latest/geo_index/kdtree/index.html)
/// @examplesIf requireNamespace("sf", quietly = TRUE) && requireNamespace("geoarrow", quietly = TRUE)
/// fp <- system.file("shape/nc.shp", package = "sf")
/// nc <- as.data.frame(read_shapefile(fp))
/// pts <- ga_centroid(nc$geometry)
/// idx <- KDTree$new(pts)
///
/// # every centroid within half a degree of every other
/// head(as.vector(idx$within(pts, 0.5)), 3)
///
/// # a single lookup is an array of one
/// as.vector(idx$within(ga_xy(-78.6, 35.8), 0.5))[[1]]
#[extendr]
impl KDTree {
    /// Build the index. A bigger `node_size` builds quicker but searches slower.
    fn new(geometry: Robj, #[extendr(default = "64")] node_size: i32) -> anyhow::Result<Self> {
        if node_size < 2 {
            anyhow::bail!("`node_size` must be at least 2");
        }

        let chunks = as_point_chunks(geometry).map_err(|e| anyhow::anyhow!("{e}"))?;
        let n = chunks.iter().map(|c| c.len()).sum();
        let node_size = u16::try_from(node_size).unwrap_or(DEFAULT_KDTREE_NODE_SIZE);
        let (tree, positions) = add_points(&chunks, node_size)?;

        Ok(Self { tree, positions, n })
    }

    /// Which points fall inside each geometry's box
    ///
    /// @param geometry a GeoArrow array to look up
    /// @returns a list array of 1 based row numbers, the same length as
    ///   `geometry`
    fn range(&self, geometry: Robj) -> anyhow::Result<Robj> {
        let chunks = as_geometry_chunks(geometry).map_err(|e| anyhow::anyhow!("{e}"))?;
        let rects = as_rects(&chunks).map_err(|e| anyhow::anyhow!("{e}"))?;

        let mut bldr = ListBuilder::new(UInt32Builder::new());
        for rect in rects {
            match rect {
                Some(rect) => {
                    let found =
                        self.tree
                            .range(rect.min().x, rect.min().y, rect.max().x, rect.max().y);
                    self.push_rows(&mut bldr, found);
                }
                None => bldr.append(false),
            }
        }

        bldr.finish()
            .into_data()
            .into_arrow_robj()
            .map_err(|e| anyhow::anyhow!("{e}"))
    }

    /// Which points are within `r` of each point, measured flat in coordinate units
    ///
    /// @param geometry a GeoArrow point array to look up
    /// @param r the radius; length 1 or the same length as `geometry`
    /// @returns a list array of 1 based row numbers, the same length as
    ///   `geometry`
    fn within(&self, geometry: Robj, r: Robj) -> anyhow::Result<Robj> {
        let chunks = as_point_chunks(geometry).map_err(|e| anyhow::anyhow!("{e}"))?;
        let n = chunks.iter().map(|c| c.len()).sum();
        let radii = try_float_array(r, "r").map_err(|e| anyhow::anyhow!("{e}"))?;
        check_recycle_len(radii.len(), n, "r").map_err(|e| anyhow::anyhow!("{e}"))?;

        let mut bldr = ListBuilder::new(UInt32Builder::new());
        let mut radii = radii.iter().cycle();

        for chunk in &chunks {
            for item in chunk.iter() {
                let radius = radii.next().flatten();
                match (item, radius) {
                    (Some(Ok(point)), Some(radius)) if radius >= 0.0 => {
                        let point = point.to_point();
                        if point.x().is_finite() && point.y().is_finite() {
                            let found = self.tree.within(point.x(), point.y(), radius);
                            self.push_rows(&mut bldr, found);
                        } else {
                            bldr.append(false);
                        }
                    }
                    _ => bldr.append(false),
                }
            }
        }

        bldr.finish()
            .into_data()
            .into_arrow_robj()
            .map_err(|e| anyhow::anyhow!("{e}"))
    }

    /// How many rows went in
    ///
    /// @returns the length of the array the index was built from
    fn size(&self) -> i32 {
        self.n as i32
    }

    /// How many rows made it into the tree, skipping null and empty points
    ///
    /// @returns the number of indexed rows
    fn n_indexed(&self) -> i32 {
        self.positions.len() as i32
    }
}

impl KDTree {
    /// Append one query's hits as a list element, in increasing row order.
    fn push_rows(&self, bldr: &mut ListBuilder<UInt32Builder>, found: Vec<u32>) {
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
}

extendr_module! {
    mod kdtree;
    impl KDTree;
}
