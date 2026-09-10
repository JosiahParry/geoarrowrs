use arrow::array::{Array, Float64Builder, Int32Builder, ListBuilder};
use arrow_extendr::IntoArrowRobj;
use extendr_api::prelude::*;
use geo::algorithm::dbscan::Dbscan;
use geo::algorithm::kmeans::KMeans;
use geo::algorithm::outlier_detection::OutlierDetection;
use geo::{Geometry, MultiPoint, Point};
use geoarrow_array::GeoArrowArray;

use crate::{as_geo_geometries, as_geometry_chunks, check_recycle_len, try_float_array};

/// Clustering is defined over a set of points, so read each row as a multipoint.
fn as_points(g: &Geometry<f64>) -> Option<MultiPoint<f64>> {
    match g {
        Geometry::MultiPoint(mp) => Some(mp.clone()),
        Geometry::Point(p) => Some(MultiPoint(vec![*p])),
        Geometry::LineString(ls) => Some(MultiPoint(ls.points().collect::<Vec<Point<f64>>>())),
        _ => None,
    }
}

/// Assign points to clusters by density
///
/// Labels each point of a geometry with a cluster number, or `NA` when the
/// point is noise. Returns one list of labels per input geometry.
///
/// @details
/// DBSCAN grows a cluster from any point with at least `min_points` neighbours
/// within `eps`. Points reachable from that core join the cluster, and points
/// that never become reachable are noise. Unlike k-means it finds clusters of
/// any shape and does not need the count up front.
///
/// Cluster numbers start at 1 and mean nothing beyond grouping. Both `eps` and
/// `min_points` are recycled against `geometry`. A row that is not point based,
/// or is null, comes back null.
///
/// @param geometry a GeoArrow multipoint array
/// @param eps how close two points must be to be neighbours; length 1 or the
///   same length as `geometry`
/// @param min_points how many neighbours a point needs to seed a cluster;
///   length 1 or the same length as `geometry`
/// @returns a list array of integer labels, one list per input geometry
/// @export
/// @family cluster
/// @references [Dbscan](https://docs.rs/geo/latest/geo/algorithm/dbscan/trait.Dbscan.html)
/// @examplesIf requireNamespace("sf", quietly = TRUE) && requireNamespace("geoarrow", quietly = TRUE)
/// pts <- sf::st_multipoint(cbind(
///   c(0, 0.1, 0.2, 5, 5.1, 5.2),
///   c(0, 0.1, 0.2, 5, 5.1, 5.2)
/// ))
/// g <- geoarrow::as_geoarrow_array(sf::st_sfc(pts))
///
/// nanoarrow::convert_array(ga_dbscan(g, eps = 1, min_points = 2))
#[extendr]
fn ga_dbscan(geometry: Robj, eps: Robj, min_points: Robj) -> anyhow::Result<Robj> {
    let epsilons = try_float_array(eps, "eps").map_err(|e| anyhow::anyhow!("{e}"))?;
    let minima = try_float_array(min_points, "min_points").map_err(|e| anyhow::anyhow!("{e}"))?;

    let chunks = as_geometry_chunks(geometry).map_err(|e| anyhow::anyhow!("{e}"))?;
    let n: usize = chunks.iter().map(|c| c.len()).sum();
    check_recycle_len(epsilons.len(), n, "eps").map_err(|e| anyhow::anyhow!("{e}"))?;
    check_recycle_len(minima.len(), n, "min_points").map_err(|e| anyhow::anyhow!("{e}"))?;

    let mut bldr = ListBuilder::new(Int32Builder::new());

    for chunk in &chunks {
        let geoms = as_geo_geometries(chunk.as_ref()).map_err(|e| anyhow::anyhow!("{e}"))?;
        for ((geom, eps), min_pts) in geoms
            .into_iter()
            .zip(epsilons.iter().cycle())
            .zip(minima.iter().cycle())
        {
            let points = geom.as_ref().and_then(as_points);
            match (points, eps, min_pts) {
                (Some(mp), Some(eps), Some(min_pts)) if min_pts >= 1.0 => {
                    for label in mp.dbscan(eps, min_pts as usize) {
                        match label {
                            Some(id) => bldr.values().append_value(id as i32 + 1),
                            None => bldr.values().append_null(),
                        }
                    }
                    bldr.append(true);
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

/// Assign points to a fixed number of clusters
///
/// Splits each geometry's points into `k` clusters by nearest centre. Returns
/// one list of labels per input geometry.
///
/// @details
/// k-means always produces exactly `k` clusters and no noise, so every point
/// gets a label. It favours round, similarly sized clusters; use [ga_dbscan()]
/// when the shapes are irregular or the count is unknown.
///
/// The algorithm starts from a random seed, so results vary between runs
/// unless `seed` is given. A row with fewer than `k` points cannot be split
/// and comes back null, as does a row that is not point based or is null.
///
/// @param geometry a GeoArrow multipoint array
/// @param k how many clusters to produce; length 1 or the same length as
///   `geometry`
/// @param seed a seed for reproducible starts, or `NULL` to vary each run
/// @returns a list array of integer labels, one list per input geometry
/// @export
/// @family cluster
/// @references [KMeans](https://docs.rs/geo/latest/geo/algorithm/kmeans/trait.KMeans.html)
/// @examplesIf requireNamespace("sf", quietly = TRUE) && requireNamespace("geoarrow", quietly = TRUE)
/// pts <- sf::st_multipoint(cbind(
///   c(0, 0.1, 0.2, 5, 5.1, 5.2),
///   c(0, 0.1, 0.2, 5, 5.1, 5.2)
/// ))
/// g <- geoarrow::as_geoarrow_array(sf::st_sfc(pts))
///
/// nanoarrow::convert_array(ga_kmeans(g, k = 2, seed = 1))
#[extendr]
fn ga_kmeans(
    geometry: Robj,
    k: Robj,
    #[extendr(default = "NULL")] seed: Nullable<f64>,
) -> anyhow::Result<Robj> {
    let ks = try_float_array(k, "k").map_err(|e| anyhow::anyhow!("{e}"))?;
    let chunks = as_geometry_chunks(geometry).map_err(|e| anyhow::anyhow!("{e}"))?;
    let n: usize = chunks.iter().map(|c| c.len()).sum();
    check_recycle_len(ks.len(), n, "k").map_err(|e| anyhow::anyhow!("{e}"))?;

    let seed = match seed {
        Nullable::NotNull(s) => Some(s as u64),
        Nullable::Null => None,
    };

    let mut bldr = ListBuilder::new(Int32Builder::new());

    for chunk in &chunks {
        let geoms = as_geo_geometries(chunk.as_ref()).map_err(|e| anyhow::anyhow!("{e}"))?;
        for (geom, k) in geoms.into_iter().zip(ks.iter().cycle()) {
            let points = geom.as_ref().and_then(as_points);
            let labels = match (points, k) {
                (Some(mp), Some(k)) if k >= 1.0 => match seed {
                    Some(seed) => mp.kmeans_with_seed(k as usize, seed).ok(),
                    None => mp.kmeans(k as usize).ok(),
                },
                _ => None,
            };
            match labels {
                Some(labels) => {
                    for id in labels {
                        bldr.values().append_value(id as i32 + 1);
                    }
                    bldr.append(true);
                }
                None => bldr.append(false),
            }
        }
    }

    bldr.finish()
        .into_data()
        .into_arrow_robj()
        .map_err(|e| anyhow::anyhow!("{e}"))
}

/// Score how much each point looks like an outlier
///
/// Returns the local outlier factor of every point in a geometry, one list per
/// input geometry.
///
/// @details
/// A score near 1 means a point sits at the same density as its neighbours.
/// Scores meaningfully above 1 mean it is in a sparser neighbourhood than they
/// are, which is what marks an outlier. There is no universal cutoff, so
/// compare scores within a dataset rather than against a fixed threshold.
///
/// `k_neighbours` sets how many neighbours define the local neighbourhood.
/// Small values react to fine structure and large values smooth it away. A
/// row that is not point based, or is null, comes back null.
///
/// @param geometry a GeoArrow multipoint array
/// @param k_neighbours how many neighbours define a neighbourhood; length 1 or
///   the same length as `geometry`
/// @returns a list array of doubles, one list per input geometry
/// @export
/// @family cluster
/// @references [OutlierDetection](https://docs.rs/geo/latest/geo/algorithm/outlier_detection/trait.OutlierDetection.html)
/// @examplesIf requireNamespace("sf", quietly = TRUE) && requireNamespace("geoarrow", quietly = TRUE)
/// pts <- sf::st_multipoint(cbind(
///   c(0, 0.1, 0.2, 0.3, 9),
///   c(0, 0.1, 0.2, 0.3, 9)
/// ))
/// g <- geoarrow::as_geoarrow_array(sf::st_sfc(pts))
///
/// nanoarrow::convert_array(ga_outlier_scores(g, k_neighbours = 2))
#[extendr]
fn ga_outlier_scores(geometry: Robj, k_neighbours: Robj) -> anyhow::Result<Robj> {
    let ks = try_float_array(k_neighbours, "k_neighbours").map_err(|e| anyhow::anyhow!("{e}"))?;
    let chunks = as_geometry_chunks(geometry).map_err(|e| anyhow::anyhow!("{e}"))?;
    let n: usize = chunks.iter().map(|c| c.len()).sum();
    check_recycle_len(ks.len(), n, "k_neighbours").map_err(|e| anyhow::anyhow!("{e}"))?;

    let mut bldr = ListBuilder::new(Float64Builder::new());

    for chunk in &chunks {
        let geoms = as_geo_geometries(chunk.as_ref()).map_err(|e| anyhow::anyhow!("{e}"))?;
        for (geom, k) in geoms.into_iter().zip(ks.iter().cycle()) {
            let points = geom.as_ref().and_then(as_points);
            match (points, k) {
                (Some(mp), Some(k)) if k >= 1.0 => {
                    for score in mp.outliers(k as usize) {
                        bldr.values().append_value(score);
                    }
                    bldr.append(true);
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

extendr_module! {
    mod cluster;
    fn ga_dbscan;
    fn ga_kmeans;
    fn ga_outlier_scores;
}
