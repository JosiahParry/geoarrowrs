use arrow_extendr::IntoArrowRobj;
use extendr_api::prelude::*;
use geo::algorithm::triangulate_delaunay::DelaunayTriangulationConfig;
use geo::{
    Geometry, MultiPolygon, Triangle, TriangulateDelaunay, TriangulateDelaunayUnconstrained,
    TriangulateEarcut,
};
use geoarrow::array::MultiPolygonBuilder;
use geoarrow::datatypes::{Dimension, MultiPolygonType};
use geoarrow_array::GeoArrowArray;

use crate::{as_geo_geometries, as_geometry_chunks, check_recycle_len, try_float_array};

/// Collect triangles into the multipolygon that represents one input geometry.
fn to_multi_polygon(triangles: Vec<Triangle<f64>>) -> MultiPolygon<f64> {
    MultiPolygon(triangles.into_iter().map(|t| t.to_polygon()).collect())
}

/// geo implements LinesIter for concrete types but not for Geometry, so dispatch; a point has no edges to constrain against and yields None.
fn constrained_of(
    g: &Geometry<f64>,
    config: DelaunayTriangulationConfig<f64>,
) -> Option<Vec<Triangle<f64>>> {
    match g {
        Geometry::Polygon(v) => v.constrained_triangulation(config).ok(),
        Geometry::MultiPolygon(v) => v.constrained_triangulation(config).ok(),
        Geometry::LineString(v) => v.constrained_triangulation(config).ok(),
        Geometry::MultiLineString(v) => v.constrained_triangulation(config).ok(),
        Geometry::Line(v) => v.constrained_triangulation(config).ok(),
        Geometry::Rect(v) => v.constrained_triangulation(config).ok(),
        Geometry::Triangle(v) => v.constrained_triangulation(config).ok(),
        _ => None,
    }
}

/// Triangulate polygons with the earcut algorithm
///
/// Returns one multipolygon per input geometry, whose parts are that
/// geometry's triangles. The output has the same length as the input, so a
/// triangulated geometry stays aligned with the row it came from.
///
/// @details
/// Earcut is defined for polygons only. A multipolygon is triangulated part by
/// part and the triangles are merged into a single multipolygon. Any other
/// geometry type, a null geometry, or a polygon that cannot be triangulated
/// becomes a null element.
///
/// Earcut is fast and respects interior rings, but the triangles it produces
/// are not Delaunay. Use [ga_triangulate_delaunay()] when triangle quality
/// matters.
///
/// @param x a GeoArrow polygon or multipolygon array
/// @returns a GeoArrow multipolygon array of the same length as `x`
/// @export
/// @family triangulate
/// @references [TriangulateEarcut](https://docs.rs/geo/latest/geo/algorithm/triangulate_earcut/trait.TriangulateEarcut.html)
/// @examplesIf requireNamespace("sf", quietly = TRUE) && requireNamespace("geoarrow", quietly = TRUE)
/// fp <- system.file("shape/nc.shp", package = "sf")
/// nc <- as.data.frame(read_shapefile(fp))
///
/// # one multipolygon of triangles per county, so length is preserved
/// tri <- ga_triangulate_earcut(nc$geometry)
/// tri$length
/// head(as.vector(ga_n_coords(tri)), 3)
#[extendr]
fn ga_triangulate_earcut(x: Robj) -> extendr_api::Result<Robj> {
    let chunks = as_geometry_chunks(x)?;
    let metadata = chunks[0].data_type().metadata().clone();
    let mut bldr = MultiPolygonBuilder::new(MultiPolygonType::new(Dimension::XY, metadata));

    for chunk in &chunks {
        for geom in as_geo_geometries(chunk.as_ref())? {
            let triangles = match geom {
                Some(Geometry::Polygon(p)) => Some(p.earcut_triangles()),
                Some(Geometry::MultiPolygon(mp)) => {
                    Some(mp.iter().flat_map(|p| p.earcut_triangles()).collect())
                }
                _ => None,
            };
            match triangles {
                Some(t) => bldr
                    .push_multi_polygon(Some(&to_multi_polygon(t)))
                    .map_err(|e| Error::Other(e.to_string()))?,
                None => bldr
                    .push_multi_polygon(None::<&MultiPolygon<f64>>)
                    .map_err(|e| Error::Other(e.to_string()))?,
            }
        }
    }

    bldr.finish().into_arrow_robj()
}

/// Delaunay triangulation
///
/// Returns one multipolygon per input geometry, whose parts are that
/// geometry's triangles. The output has the same length as the input.
///
/// @details
/// A constrained triangulation keeps the input's edges and returns only the
/// triangles that fall inside the geometry. An unconstrained triangulation
/// triangulates the convex hull of the input's vertices, ignoring its edges,
/// so it may cross holes and concavities.
///
/// A null geometry, or one that cannot be triangulated, becomes a null
/// element.
///
/// @param x a GeoArrow geometry array
/// @param constrained whether to constrain the triangulation to the input's
///   edges. `TRUE` by default
/// @param snap_radius coordinates closer together than this are snapped to the
///   same position; length 1 or the same length as `x`. `geo` uses 1e-4 by
///   default
/// @returns a GeoArrow multipolygon array of the same length as `x`
/// @export
/// @family triangulate
/// @references [TriangulateDelaunay](https://docs.rs/geo/latest/geo/algorithm/triangulate_delaunay/trait.TriangulateDelaunay.html)
/// @examplesIf requireNamespace("sf", quietly = TRUE) && requireNamespace("geoarrow", quietly = TRUE)
/// fp <- system.file("shape/nc.shp", package = "sf")
/// nc <- as.data.frame(read_shapefile(fp))
///
/// # constrained keeps the triangles inside the county boundary
/// tri <- ga_triangulate_delaunay(nc$geometry)
/// tri$length
///
/// # unconstrained fills the convex hull instead, so it covers more
/// hull <- ga_triangulate_delaunay(nc$geometry, constrained = FALSE)
/// head(as.vector(ga_unsigned_area(tri)), 3)
/// head(as.vector(ga_unsigned_area(hull)), 3)
#[extendr]
fn ga_triangulate_delaunay(
    x: Robj,
    #[extendr(default = "TRUE")] constrained: bool,
    #[extendr(default = "1e-4")] snap_radius: Robj,
) -> extendr_api::Result<Robj> {
    let radii = try_float_array(snap_radius, "snap_radius")?;
    let chunks = as_geometry_chunks(x)?;
    let n: usize = chunks.iter().map(|c| c.len()).sum();
    check_recycle_len(radii.len(), n, "snap_radius")?;

    let metadata = chunks[0].data_type().metadata().clone();
    let mut bldr = MultiPolygonBuilder::new(MultiPolygonType::new(Dimension::XY, metadata));

    for chunk in &chunks {
        for (geom, radius) in as_geo_geometries(chunk.as_ref())?
            .into_iter()
            .zip(radii.iter().cycle())
        {
            let triangles = match (geom, radius) {
                (Some(g), Some(r)) => {
                    let config = DelaunayTriangulationConfig { snap_radius: r };
                    if constrained {
                        constrained_of(&g, config)
                    } else {
                        g.unconstrained_triangulation().ok()
                    }
                }
                _ => None,
            };
            match triangles {
                Some(t) => bldr
                    .push_multi_polygon(Some(&to_multi_polygon(t)))
                    .map_err(|e| Error::Other(e.to_string()))?,
                None => bldr
                    .push_multi_polygon(None::<&MultiPolygon<f64>>)
                    .map_err(|e| Error::Other(e.to_string()))?,
            }
        }
    }

    bldr.finish().into_arrow_robj()
}

extendr_module! {
    mod triangulate;
    fn ga_triangulate_earcut;
    fn ga_triangulate_delaunay;
}
