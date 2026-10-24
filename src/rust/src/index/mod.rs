use arrow::array::{Array, ListBuilder, UInt32Builder};
use arrow_extendr::IntoArrowRobj;
use extendr_api::prelude::*;

use geo_index::rtree::sort::{HilbertSort, STRSort};
use geo_index::rtree::{DEFAULT_RTREE_NODE_SIZE, RTreeBuilder, RTreeIndex, RTreeRef};
use geoarrow_array::GeoArrowArray;
mod file;
mod kdtree;

use crate::as_geometry_chunks;
use crate::envelope::as_rects;
use file::IndexFile;

// Spatial index over a geometry array
#[extendr]
pub struct RTree {
    /// The tree with the original row of each entry, since null and empty rows are never added.
    file: IndexFile,
}

/// A spatial index over a geometry array
///
/// Indexes the bounding box of each geometry so that queries can skip the rows
/// that cannot match.
///
/// @details
/// The tree is a packed Hilbert R-tree, the same structure FlatGeobuf stores
/// on disk. You build it once and it is immutable, so it pays off when you
/// query a set of geometries repeatedly.
///
/// Every query is a bounding box test, not an exact one. Two geometries whose
/// boxes overlap need not themselves intersect, so treat results as candidates
/// and confirm with [ga_intersects()] when exactness matters. You narrow to
/// candidates first so you can run the exact test on a handful of rows rather
/// than all of them.
///
/// Rows with a null or empty geometry have no bounding box, so `new()` leaves
/// them out of the tree and a query never returns them. Row numbers always
/// refer to positions in the original array.
///
/// `write()` saves the index to a file and `RTree$read()` opens it again, so
/// you build it once and reuse it across sessions. With `mmap = TRUE`,
/// `read()` maps the file instead of reading it, so the index opens at once
/// and each query loads only the parts of the tree it visits.
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
/// # candidate rows whose box meets each county, still to confirm exactly
/// head(as.vector(idx$search(nc$geometry)), 3)
///
/// # the three counties nearest each centroid
/// head(as.vector(idx$neighbors(ga_centroid(nc$geometry), k = 3)), 3)
///
/// # save the index and open it again without reading the whole file
/// path <- tempfile(fileext = ".gaix")
/// idx$write(path)
/// saved <- RTree$read(path, mmap = TRUE)
/// head(as.vector(saved$search(nc$geometry)), 3)
#[extendr]
impl RTree {
    /// Build the index. Use `node_size` to set how many entries share a tree
    /// node; with larger values you build faster and query slower. Use `sort`
    /// to pick the packing order, either `"hilbert"` or `"str"`.
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
        let rects = as_rects(crate::threads::Threads::get(), &chunks)
            .map_err(|e| anyhow::anyhow!("{e}"))?;

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

        Ok(Self {
            file: IndexFile::new(n, &positions, &tree.into_inner()),
        })
    }

    /// Read an index that `write()` saved
    ///
    /// @details
    /// With `mmap = TRUE`, `read()` maps the file instead of reading it into
    /// memory, so it opens at once whatever the file's size and each query
    /// loads only the parts of the tree it visits. Do not change or delete
    /// the file while you use a mapped index, or R may crash.
    ///
    /// @param path the file `write()` saved
    /// @param mmap whether to map the file rather than read it into memory
    /// @returns an `RTree` object
    fn read(path: &str, #[extendr(default = "FALSE")] mmap: bool) -> anyhow::Result<Self> {
        let file = IndexFile::open(path, mmap)?;
        {
            let bytes = file.tree();
            let tree = RTreeRef::<f64>::try_new(&bytes)
                .map_err(|e| anyhow::anyhow!("The file does not hold an R-tree: {e}"))?;
            if tree.num_items() as usize != file.positions().len() {
                anyhow::bail!("The index file is corrupt: its tree and row positions disagree");
            }
        }
        Ok(Self { file })
    }

    /// Save the index to a file
    ///
    /// @details
    /// `write()` saves the tree with the row each entry came from, so
    /// `RTree$read()` restores an index that answers every query the same
    /// way. The tree keeps the layout of the JavaScript flatbush library.
    ///
    /// @param path the file to write
    /// @returns `NULL`
    fn write(&self, path: &str) -> anyhow::Result<()> {
        self.file.write(path)
    }

    /// Which rows have a bounding box overlapping each geometry
    ///
    /// Returns one list of candidate row numbers per element of `geometry`,
    /// row for row with the query array. This is the shape a spatial join
    /// needs.
    ///
    /// @details
    /// `search()` accepts anything with a bounding box: a point array, a
    /// polygon array, or the box array [ga_envelope()] produces. It takes boxes
    /// as they are and reduces everything else to its envelope first.
    ///
    /// This is a bounding box test, not an exact one, so each list holds
    /// candidates to confirm with [ga_intersects()] or another predicate.
    /// `search()` returns a null rather than an empty list for a null or empty
    /// query geometry.
    ///
    /// @param geometry a GeoArrow array to look up
    /// @returns a list array of 1 based row numbers, the same length as
    ///   `geometry`
    fn search(&self, geometry: Robj) -> anyhow::Result<Robj> {
        let chunks = as_geometry_chunks(geometry).map_err(|e| anyhow::anyhow!("{e}"))?;
        let rects = as_rects(crate::threads::Threads::get(), &chunks)
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        let bytes = self.file.tree();
        let tree = RTreeRef::<f64>::try_new(&bytes).map_err(|e| anyhow::anyhow!("{e}"))?;
        let positions = self.file.positions();

        let mut bldr = ListBuilder::new(UInt32Builder::new());
        for rect in rects {
            match rect {
                Some(rect) => {
                    let found = tree.search(rect.min().x, rect.min().y, rect.max().x, rect.max().y);
                    let mut rows = found
                        .into_iter()
                        .filter_map(|i| positions.get(i as usize).copied())
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

    /// Which rows are nearest each geometry, closest first
    ///
    /// @details
    /// `neighbors()` measures distance from the centre of each query geometry
    /// to the bounding box of the indexed one, so it returns candidates to
    /// confirm with [ga_dist_euclidean_pairwise()] when exactness matters. Use
    /// `k` to cap how many rows it returns per row and `max_distance` to cap
    /// how far it searches.
    ///
    /// Because `neighbors()` takes an array rather than one point at a time,
    /// you can do a nearest neighbour join in one call.
    ///
    /// @param geometry a GeoArrow array to look up
    /// @param k the most rows to return per query, or `NULL` for no limit
    /// @param max_distance the furthest to search, or `NULL` for no limit
    /// @returns a list array of 1 based row numbers, the same length as
    ///   `geometry`
    fn neighbors(
        &self,
        geometry: Robj,
        #[extendr(default = "NULL")] k: Option<i32>,
        #[extendr(default = "NULL")] max_distance: Option<f64>,
    ) -> anyhow::Result<Robj> {
        let k = match k {
            Some(k) if k < 1 => anyhow::bail!("`k` must be at least 1"),
            Some(k) => Some(k as usize),
            None => None,
        };

        let chunks = as_geometry_chunks(geometry).map_err(|e| anyhow::anyhow!("{e}"))?;
        let rects = as_rects(crate::threads::Threads::get(), &chunks)
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        let bytes = self.file.tree();
        let tree = RTreeRef::<f64>::try_new(&bytes).map_err(|e| anyhow::anyhow!("{e}"))?;
        let positions = self.file.positions();
        let mut bldr = ListBuilder::new(UInt32Builder::new());

        for rect in rects {
            match rect {
                Some(rect) => {
                    let centre = rect.center();
                    let found = tree.neighbors(centre.x, centre.y, k, max_distance);
                    // neighbors returns nearest first, so keep that order
                    for i in found {
                        if let Some(row) = positions.get(i as usize) {
                            bldr.values().append_value(row + 1);
                        }
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

    /// The number of rows you built the index over
    ///
    /// @returns the length of the array you built the index from
    fn size(&self) -> i32 {
        self.file.n() as i32
    }

    /// The number of rows the tree actually holds
    ///
    /// @details
    /// Lower than `size()` when the array held null or empty geometries, which
    /// have no bounding box to index.
    ///
    /// @returns the number of indexed rows
    fn n_indexed(&self) -> i32 {
        self.file.positions().len() as i32
    }
}

extendr_module! {
    mod index;
    impl RTree;
    use kdtree;
}
