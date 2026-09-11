use arrow::array::{Array, ArrayRef, Float64Array, ListArray, StructArray, UInt32Array};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::{DataType, Field, Fields};
use arrow_extendr::IntoArrowRobj;
use extendr_api::prelude::*;
use geo::{Distance, Euclidean, Geometry};
use geo_index::rtree::{RTree as GeoRTree, RTreeIndex};
use rayon::prelude::*;
use std::sync::Arc;

use super::{as_geoms_and_rects, index_rects};
use crate::threads::with_pool;

/// The exact distance from one geometry to an indexed row.
fn distance_to(geom: &Geometry<f64>, ys: &[Option<Geometry<f64>>], row: u32) -> Option<f64> {
    ys.get(row as usize)?
        .as_ref()
        .map(|y| Euclidean.distance(geom, y))
}

/// The rows of `y` nearest one geometry, nearest first, each with its distance.
fn nearest(
    geom: &Geometry<f64>,
    rect: &geo::Rect<f64>,
    tree: &GeoRTree<f64>,
    positions: &[u32],
    ys: &[Option<Geometry<f64>>],
    k: usize,
    max_distance: Option<f64>,
) -> Vec<(u32, f64)> {
    // the tree ranks by distance to the box, so its k rows only bound the true k
    let centre = rect.center();
    let seed = tree.neighbors(centre.x, centre.y, Some(k), None);
    let mut bound = f64::INFINITY;
    if seed.len() >= k {
        let mut seen = seed
            .iter()
            .filter_map(|i| positions.get(*i as usize).copied())
            .filter_map(|row| distance_to(geom, ys, row))
            .collect::<Vec<_>>();
        seen.sort_by(f64::total_cmp);
        if let Some(d) = seen.get(k - 1) {
            bound = *d;
        }
    }
    if let Some(limit) = max_distance {
        bound = bound.min(limit);
    }

    // nothing closer than `bound` can sit outside the box grown by `bound`
    let candidates = if bound.is_finite() {
        tree.search(
            rect.min().x - bound,
            rect.min().y - bound,
            rect.max().x + bound,
            rect.max().y + bound,
        )
        .into_iter()
        .filter_map(|i| positions.get(i as usize).copied())
        .collect::<Vec<_>>()
    } else {
        positions.to_vec()
    };

    let mut found = candidates
        .into_iter()
        .filter_map(|row| {
            distance_to(geom, ys, row)
                .filter(|d| *d <= bound)
                .map(|d| (row + 1, d))
        })
        .collect::<Vec<_>>();
    found.sort_by(|a, b| a.1.total_cmp(&b.1).then(a.0.cmp(&b.0)));
    found.truncate(k);
    found
}

/// Pack the matches as one list of `{row, distance}` structs per row of `x`.
fn as_list_array(found: Vec<Option<Vec<(u32, f64)>>>) -> ListArray {
    let mut rows = Vec::new();
    let mut distances = Vec::new();
    let mut offsets = Vec::with_capacity(found.len() + 1);
    let mut valid = Vec::with_capacity(found.len());
    offsets.push(0);

    for item in found {
        match item {
            Some(matches) => {
                for (row, distance) in matches {
                    rows.push(row);
                    distances.push(distance);
                }
                valid.push(true);
            }
            None => valid.push(false),
        }
        offsets.push(rows.len() as i32);
    }

    let fields = Fields::from(vec![
        Field::new("row", DataType::UInt32, false),
        Field::new("distance", DataType::Float64, false),
    ]);
    let values = StructArray::new(
        fields.clone(),
        vec![
            Arc::new(UInt32Array::from(rows)) as ArrayRef,
            Arc::new(Float64Array::from(distances)) as ArrayRef,
        ],
        None,
    );

    ListArray::new(
        Arc::new(Field::new("item", DataType::Struct(fields), true)),
        OffsetBuffer::new(offsets.into()),
        Arc::new(values),
        Some(NullBuffer::from(valid)),
    )
}

/// Find the rows of `y` nearest each row of `x`
///
/// Returns the `k` rows of `y` closest to each row of `x`, nearest first, each
/// paired with the distance between them. This is the sparse form of a nearest
/// neighbour search, and the shape [ga_knn_join()] needs.
///
/// @details
/// Distance is Euclidean and measured between the geometries themselves, not
/// between their bounding boxes, so the nearest edge of a polygon counts rather
/// than the corner of the box around it. `y` is indexed in a packed Hilbert
/// R-tree, which narrows the search to the rows that can win before any exact
/// distance is computed, and the answer is the same as comparing every pair.
///
/// A row matches fewer than `k` rows only when `max_distance` rules the rest
/// out or `y` is shorter than `k`. A null or empty geometry in `x` gives a null
/// element, and one in `y` is never returned.
///
/// @param x a GeoArrow geometry array
/// @param y a GeoArrow geometry array
/// @param k how many rows of `y` to return per row of `x`
/// @param max_distance the furthest a match may be, or `NULL` for no limit
/// @returns a list array of `row` and `distance` pairs, the same length as `x`,
///   where `row` is a 1 based row number into `y`
/// @export
/// @family index
/// @examplesIf requireNamespace("sf", quietly = TRUE) && requireNamespace("geoarrow", quietly = TRUE)
/// nc <- as.data.frame(read_shapefile(
///   system.file("shape/nc.shp", package = "sf")
/// ))
/// sites <- ga_xy(c(-78.6, -80.8), c(35.8, 35.2))
///
/// # the three counties nearest each site, with their distances
/// as.vector(ga_sparse_knn(sites, nc$geometry, k = 3))
#[extendr]
fn ga_sparse_knn(
    x: Robj,
    y: Robj,
    #[extendr(default = "1")] k: i32,
    #[extendr(default = "NULL")] max_distance: Option<f64>,
) -> extendr_api::Result<Robj> {
    if k < 1 {
        return Err(Error::Other("`k` must be at least 1".to_string()));
    }
    if max_distance.is_some_and(|d| d < 0.0) {
        return Err(Error::Other(
            "`max_distance` must not be negative".to_string(),
        ));
    }
    let k = k as usize;

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
                    Some(nearest(geom, rect, &tree, &positions, &ys, k, max_distance))
                })
                .collect::<Vec<_>>()
        }),
        // no row of `y` has a box, so nothing matches, but the shape still has to line up
        None => x_rects
            .iter()
            .map(|rect| rect.as_ref().map(|_| Vec::new()))
            .collect(),
    };

    as_list_array(found).into_data().into_arrow_robj()
}

extendr_module! {
    mod knn;
    fn ga_sparse_knn;
}
