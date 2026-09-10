use arrow_extendr::IntoArrowRobj;
use extendr_api::prelude::*;
use geo::algorithm::voronoi::{Voronoi, VoronoiClip, VoronoiParams};
use geo::{Geometry, Line, LineString, MultiLineString, MultiPolygon, Polygon};
use geoarrow::array::{MultiLineStringBuilder, MultiPolygonBuilder};
use geoarrow::datatypes::{Dimension, MultiLineStringType, MultiPolygonType};
use geoarrow_array::GeoArrowArray;

use crate::{as_geo_geometries, as_geometry_chunks, check_recycle_len, try_float_array};

/// Collect the cells of one diagram into the multipolygon that represents one input geometry.
fn to_multi_polygon(cells: Vec<Polygon<f64>>) -> MultiPolygon<f64> {
    MultiPolygon(cells)
}

/// Each Voronoi edge is a bare segment, so widen it to a two point linestring.
fn to_multi_line_string(edges: Vec<Line<f64>>) -> MultiLineString<f64> {
    MultiLineString(
        edges
            .into_iter()
            .map(|l| LineString::new(vec![l.start, l.end]))
            .collect(),
    )
}

/// Read an optional boundary argument as one clip polygon per row, recycled.
fn boundary_polygons(
    boundary: Robj,
    n: usize,
) -> extendr_api::Result<Option<Vec<Option<Polygon<f64>>>>> {
    if boundary.is_null() {
        return Ok(None);
    }

    let chunks = as_geometry_chunks(boundary)?;
    let mut out = Vec::new();
    for chunk in &chunks {
        for geom in as_geo_geometries(chunk.as_ref())? {
            out.push(match geom {
                Some(Geometry::Polygon(p)) => Some(p),
                _ => None,
            });
        }
    }

    check_recycle_len(out.len(), n, "boundary")?;
    Ok(Some(out))
}

/// Build the clip mode for one row, preferring an explicit boundary polygon over the named mode.
fn clip_for<'a>(
    mode: &str,
    boundary: Option<&'a Polygon<f64>>,
) -> extendr_api::Result<VoronoiClip<'a, f64>> {
    if let Some(p) = boundary {
        return Ok(VoronoiClip::Polygon(p));
    }
    match mode {
        "padded" => Ok(VoronoiClip::Padded),
        "envelope" => Ok(VoronoiClip::Envelope),
        other => Err(Error::Other(format!(
            "`clip` must be either \"padded\" or \"envelope\", got \"{other}\""
        ))),
    }
}

/// Compute Voronoi cells from the vertices of geometries
///
/// Returns one multipolygon per input geometry, whose parts are the Voronoi
/// cells of that geometry's vertices. The output has the same length as the
/// input.
///
/// @details
/// Every vertex of a geometry is treated as a site, so a multipoint of `k`
/// points yields `k` cells. A Voronoi diagram is unbounded, so the cells are
/// clipped: `"padded"` uses a box with 50 percent padding around the sites,
/// matching PostGIS `ST_VoronoiPolygons`, and `"envelope"` uses their exact
/// bounding box. Passing `boundary` clips to an arbitrary polygon instead,
/// which is the usual way to cut a diagram to a study area.
///
/// A geometry with fewer than two distinct vertices, or one whose vertices are
/// all collinear, has no cells and comes back as an empty multipolygon rather
/// than a null. Use [voronoi_edges()] for the collinear case, which returns
/// the perpendicular bisectors. A null geometry stays null.
///
/// @param geometry a GeoArrow geometry array whose vertices are the sites
/// @param clip how to bound the diagram, either `"padded"` or `"envelope"`.
///   Ignored when `boundary` is supplied
/// @param tolerance sites closer together than this are snapped to the same
///   position; length 1 or the same length as `geometry`
/// @param boundary an optional GeoArrow polygon array to clip to; length 1 or
///   the same length as `geometry`
/// @returns a GeoArrow multipolygon array of the same length as `geometry`
/// @export
/// @family voronoi
/// @references [Voronoi](https://docs.rs/geo/latest/geo/algorithm/voronoi/trait.Voronoi.html)
/// @examplesIf requireNamespace("sf", quietly = TRUE) && requireNamespace("geoarrow", quietly = TRUE)
/// pts <- sf::st_multipoint(cbind(c(0, 1, 1, 0), c(0, 0, 1, 1)))
/// g <- geoarrow::as_geoarrow_array(sf::st_sfc(pts))
///
/// cells <- voronoi_cells(g)
/// lengths(sf::st_as_sfc(geoarrow::as_geoarrow_vctr(cells)))
#[extendr]
fn voronoi_cells(
    geometry: Robj,
    #[extendr(default = "\"padded\"")] clip: &str,
    #[extendr(default = "0")] tolerance: Robj,
    #[extendr(default = "NULL")] boundary: Robj,
) -> extendr_api::Result<Robj> {
    let tol = try_float_array(tolerance, "tolerance")?;
    let chunks = as_geometry_chunks(geometry)?;
    let n: usize = chunks.iter().map(|c| c.len()).sum();
    check_recycle_len(tol.len(), n, "tolerance")?;
    let bounds = boundary_polygons(boundary, n)?;

    let metadata = chunks[0].data_type().metadata().clone();
    let mut bldr = MultiPolygonBuilder::new(MultiPolygonType::new(Dimension::XY, metadata));

    let mut i = 0usize;
    for chunk in &chunks {
        for (geom, t) in as_geo_geometries(chunk.as_ref())?
            .into_iter()
            .zip(tol.iter().cycle())
        {
            let bound = bounds.as_ref().and_then(|b| b[i % b.len()].as_ref());
            let cells = match (geom, t) {
                (Some(g), Some(t)) => {
                    let params = VoronoiParams::new()
                        .tolerance(t)
                        .clip(clip_for(clip, bound)?);
                    g.voronoi_cells_with_params(params).ok()
                }
                _ => None,
            };
            match cells {
                Some(c) => bldr
                    .push_multi_polygon(Some(&to_multi_polygon(c)))
                    .map_err(|e| Error::Other(e.to_string()))?,
                None => bldr
                    .push_multi_polygon(None::<&MultiPolygon<f64>>)
                    .map_err(|e| Error::Other(e.to_string()))?,
            }
            i += 1;
        }
    }

    bldr.finish().into_arrow_robj()
}

/// Compute Voronoi edges from the vertices of geometries
///
/// Returns one multilinestring per input geometry, whose parts are the
/// boundaries between that geometry's Voronoi cells. The output has the same
/// length as the input.
///
/// @details
/// Unlike [voronoi_cells()], this works on collinear sites, where the edges
/// are the perpendicular bisectors between neighbouring points. Prefer it
/// when you want the diagram's skeleton rather than closed regions.
///
/// A geometry with fewer than two distinct vertices has no edges and comes
/// back as an empty multilinestring. A null geometry stays null.
///
/// @inheritParams voronoi_cells
/// @returns a GeoArrow multilinestring array of the same length as `geometry`
/// @export
/// @family voronoi
/// @references [Voronoi](https://docs.rs/geo/latest/geo/algorithm/voronoi/trait.Voronoi.html)
/// @examplesIf requireNamespace("sf", quietly = TRUE) && requireNamespace("geoarrow", quietly = TRUE)
/// pts <- sf::st_multipoint(cbind(c(0, 1, 1, 0), c(0, 0, 1, 1)))
/// g <- geoarrow::as_geoarrow_array(sf::st_sfc(pts))
///
/// sf::st_as_sfc(geoarrow::as_geoarrow_vctr(voronoi_edges(g)))
#[extendr]
fn voronoi_edges(
    geometry: Robj,
    #[extendr(default = "\"padded\"")] clip: &str,
    #[extendr(default = "0")] tolerance: Robj,
    #[extendr(default = "NULL")] boundary: Robj,
) -> extendr_api::Result<Robj> {
    let tol = try_float_array(tolerance, "tolerance")?;
    let chunks = as_geometry_chunks(geometry)?;
    let n: usize = chunks.iter().map(|c| c.len()).sum();
    check_recycle_len(tol.len(), n, "tolerance")?;
    let bounds = boundary_polygons(boundary, n)?;

    let metadata = chunks[0].data_type().metadata().clone();
    let mut bldr = MultiLineStringBuilder::new(MultiLineStringType::new(Dimension::XY, metadata));

    let mut i = 0usize;
    for chunk in &chunks {
        for (geom, t) in as_geo_geometries(chunk.as_ref())?
            .into_iter()
            .zip(tol.iter().cycle())
        {
            let bound = bounds.as_ref().and_then(|b| b[i % b.len()].as_ref());
            let edges = match (geom, t) {
                (Some(g), Some(t)) => {
                    let params = VoronoiParams::new()
                        .tolerance(t)
                        .clip(clip_for(clip, bound)?);
                    g.voronoi_edges_with_params(params).ok()
                }
                _ => None,
            };
            match edges {
                Some(e) => bldr
                    .push_multi_line_string(Some(&to_multi_line_string(e)))
                    .map_err(|e| Error::Other(e.to_string()))?,
                None => bldr
                    .push_multi_line_string(None::<&MultiLineString<f64>>)
                    .map_err(|e| Error::Other(e.to_string()))?,
            }
            i += 1;
        }
    }

    bldr.finish().into_arrow_robj()
}

extendr_module! {
    mod voronoi;
    fn voronoi_cells;
    fn voronoi_edges;
}
