use arrow::array::{Array, Float64Builder, Int32Builder};
use arrow_extendr::IntoArrowRobj;
use extendr_api::prelude::*;
use geo::algorithm::dbscan::Dbscan;
use geo::algorithm::kmeans::KMeans;
use geo::algorithm::outlier_detection::OutlierDetection;
use geo::{Geometry, MultiPoint, Point};

use crate::{as_geo_geometries, as_geometry_chunks, try_float_array};

/// The points to cluster, and where each row's answer comes from.
type PointRows = (Vec<Point<f64>>, Vec<Option<usize>>);

/// Read the array as one point per row, so that clustering runs over the whole array.
///
/// A row that is not a single point has no place in a point cluster, so its
/// position is remembered as a gap and comes back null.
fn as_point_rows(x: Robj) -> extendr_api::Result<PointRows> {
    let chunks = as_geometry_chunks(x)?;
    let mut points = Vec::new();
    let mut slots = Vec::new();

    for chunk in &chunks {
        for geom in as_geo_geometries(chunk.as_ref())? {
            match geom {
                Some(Geometry::Point(p)) => {
                    slots.push(Some(points.len()));
                    points.push(p);
                }
                _ => slots.push(None),
            }
        }
    }

    Ok((points, slots))
}

/// Read a whole-array parameter, which is one value however many rows there are.
fn as_scalar(x: Robj, label: &'static str) -> extendr_api::Result<f64> {
    let values = try_float_array(x, label)?;
    if values.len() != 1 || values.is_null(0) {
        return Err(Error::Other(format!(
            "`{label}` must be a single value, got {}",
            values.len()
        )));
    }
    Ok(values.value(0))
}

/// Assign points to clusters by density
///
/// Labels every point in the array with a cluster number, or `NA` when the
/// point is noise. One label per row, in the order the points were given.
///
/// @details
/// DBSCAN grows a cluster from any point with at least `min_points` neighbours
/// within `eps`. Points reachable from that core join the cluster, and points
/// that never become reachable are noise. Unlike k-means it finds clusters of
/// any shape and does not need the count up front.
///
/// Clustering is over the whole array, not within each row, so `eps` and
/// `min_points` are single values rather than one per row. Cluster numbers
/// start at 1 and mean nothing beyond grouping. A row that is not a single
/// point, or is null, takes no part in the clustering and comes back null.
///
/// @param geometry a GeoArrow point array
/// @param eps how close two points must be to be neighbours
/// @param min_points how many neighbours a point needs to seed a cluster
/// @returns an integer array of cluster labels, the same length as `geometry`
/// @export
/// @family cluster
/// @references [Dbscan](https://docs.rs/geo/latest/geo/algorithm/dbscan/trait.Dbscan.html)
/// @examplesIf requireNamespace("sf", quietly = TRUE) && requireNamespace("geoarrow", quietly = TRUE)
/// g <- ga_xy(
///   c(0, 0.1, 0.2, 5, 5.1, 5.2),
///   c(0, 0.1, 0.2, 5, 5.1, 5.2)
/// )
///
/// as.vector(ga_dbscan(g, eps = 1, min_points = 2))
#[extendr]
fn ga_dbscan(geometry: Robj, eps: Robj, min_points: Robj) -> anyhow::Result<Robj> {
    let eps = as_scalar(eps, "eps").map_err(|e| anyhow::anyhow!("{e}"))?;
    let min_points = as_scalar(min_points, "min_points").map_err(|e| anyhow::anyhow!("{e}"))?;
    if min_points < 1.0 {
        anyhow::bail!("`min_points` must be at least 1");
    }

    let (points, slots) = as_point_rows(geometry).map_err(|e| anyhow::anyhow!("{e}"))?;
    let labels = MultiPoint(points).dbscan(eps, min_points as usize);

    let mut bldr = Int32Builder::with_capacity(slots.len());
    for slot in slots {
        match slot.and_then(|i| labels.get(i).copied()).flatten() {
            Some(id) => bldr.append_value(id as i32 + 1),
            None => bldr.append_null(),
        }
    }

    bldr.finish()
        .into_data()
        .into_arrow_robj()
        .map_err(|e| anyhow::anyhow!("{e}"))
}

/// Assign points to a fixed number of clusters
///
/// Splits the points in the array into `k` clusters by nearest centre. One
/// label per row, in the order the points were given.
///
/// @details
/// k-means always produces exactly `k` clusters and no noise, so every point
/// gets a label. It favours round, similarly sized clusters; use [ga_dbscan()]
/// when the shapes are irregular or the count is unknown.
///
/// Clustering is over the whole array, not within each row, so `k` is a single
/// value rather than one per row. The algorithm starts from a random seed, so
/// results vary between runs unless `seed` is given. A row that is not a single
/// point, or is null, takes no part in the clustering and comes back null.
///
/// @param geometry a GeoArrow point array
/// @param k how many clusters to produce
/// @param seed a seed for reproducible starts, or `NULL` to vary each run
/// @returns an integer array of cluster labels, the same length as `geometry`
/// @export
/// @family cluster
/// @references [KMeans](https://docs.rs/geo/latest/geo/algorithm/kmeans/trait.KMeans.html)
/// @examplesIf requireNamespace("sf", quietly = TRUE) && requireNamespace("geoarrow", quietly = TRUE)
/// g <- ga_xy(
///   c(0, 0.1, 0.2, 5, 5.1, 5.2),
///   c(0, 0.1, 0.2, 5, 5.1, 5.2)
/// )
///
/// as.vector(ga_kmeans(g, k = 2, seed = 1))
#[extendr]
fn ga_kmeans(
    geometry: Robj,
    k: Robj,
    #[extendr(default = "NULL")] seed: Option<f64>,
) -> anyhow::Result<Robj> {
    let k = as_scalar(k, "k").map_err(|e| anyhow::anyhow!("{e}"))?;
    if k < 1.0 {
        anyhow::bail!("`k` must be at least 1");
    }

    let (points, slots) = as_point_rows(geometry).map_err(|e| anyhow::anyhow!("{e}"))?;
    let mp = MultiPoint(points);
    let labels = match seed {
        Some(seed) => mp.kmeans_with_seed(k as usize, seed as u64),
        None => mp.kmeans(k as usize),
    }
    .map_err(|e| anyhow::anyhow!("{e}"))?;

    let mut bldr = Int32Builder::with_capacity(slots.len());
    for slot in slots {
        match slot.and_then(|i| labels.get(i).copied()) {
            Some(id) => bldr.append_value(id as i32 + 1),
            None => bldr.append_null(),
        }
    }

    bldr.finish()
        .into_data()
        .into_arrow_robj()
        .map_err(|e| anyhow::anyhow!("{e}"))
}

/// Score how much each point looks like an outlier
///
/// Returns the local outlier factor of every point in the array, one score per
/// row.
///
/// @details
/// A score near 1 means a point sits at the same density as its neighbours.
/// Scores meaningfully above 1 mean it is in a sparser neighbourhood than they
/// are, which is what marks an outlier. There is no universal cutoff, so
/// compare scores within a dataset rather than against a fixed threshold.
///
/// `k_neighbours` sets how many neighbours define the local neighbourhood.
/// Small values react to fine structure and large values smooth it away.
/// Scoring is over the whole array, not within each row, so it is a single
/// value. A row that is not a single point, or is null, comes back null.
///
/// @param geometry a GeoArrow point array
/// @param k_neighbours how many neighbours define a neighbourhood
/// @returns a double array of scores, the same length as `geometry`
/// @export
/// @family cluster
/// @references [OutlierDetection](https://docs.rs/geo/latest/geo/algorithm/outlier_detection/trait.OutlierDetection.html)
/// @examplesIf requireNamespace("sf", quietly = TRUE) && requireNamespace("geoarrow", quietly = TRUE)
/// g <- ga_xy(
///   c(0, 0.1, 0.2, 0.3, 9),
///   c(0, 0.1, 0.2, 0.3, 9)
/// )
///
/// round(as.vector(ga_outlier_scores(g, k_neighbours = 2)), 2)
#[extendr]
fn ga_outlier_scores(geometry: Robj, k_neighbours: Robj) -> anyhow::Result<Robj> {
    let k = as_scalar(k_neighbours, "k_neighbours").map_err(|e| anyhow::anyhow!("{e}"))?;
    if k < 1.0 {
        anyhow::bail!("`k_neighbours` must be at least 1");
    }

    let (points, slots) = as_point_rows(geometry).map_err(|e| anyhow::anyhow!("{e}"))?;
    let scores = MultiPoint(points).outliers(k as usize);

    let mut bldr = Float64Builder::with_capacity(slots.len());
    for slot in slots {
        match slot.and_then(|i| scores.get(i).copied()) {
            Some(score) => bldr.append_value(score),
            None => bldr.append_null(),
        }
    }

    bldr.finish()
        .into_data()
        .into_arrow_robj()
        .map_err(|e| anyhow::anyhow!("{e}"))
}

extendr_module! {
    mod cluster;
    fn ga_dbscan;
    fn ga_kmeans;
    fn ga_outlier_scores;
}
