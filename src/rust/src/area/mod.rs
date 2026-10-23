use arrow::array::Float64Builder;
use arrow_extendr::IntoArrowRobj;
use extendr_api::prelude::*;
use geo::{Area, ChamberlainDuquetteArea, Euclidean, GeodesicArea, Geometry, Length, Polygon};
use geoarrow_array::GeoArrowArray;

use crate::{as_geo_geometries, as_geometry_chunks};

/// Signed and unsigned planar area
///
/// `ga_signed_area()` returns positive values for counter-clockwise winding and
/// negative values for clockwise winding. `ga_unsigned_area()` always returns a
/// non-negative value.
///
/// @param x a GeoArrow geometry array
/// @returns a double vector of area values in the units of the coordinate system
/// @export
/// @rdname area
/// @family area
/// @references [Area](https://docs.rs/geo/latest/geo/algorithm/area/trait.Area.html)
/// @examplesIf requireNamespace("sf", quietly = TRUE) && requireNamespace("geoarrow", quietly = TRUE)
/// fp <- system.file("shape/nc.shp", package = "sf")
/// nc <- as.data.frame(read_shapefile(fp))
///
/// # ga_signed_area() returns negative values for nc, whose rings wind clockwise
/// head(as.vector(ga_signed_area(nc$geometry)))
/// head(as.vector(ga_unsigned_area(nc$geometry)))
#[extendr]
fn ga_signed_area(x: Robj) -> extendr_api::Result<Robj> {
    let chunks = as_geometry_chunks(x)?;
    let n = chunks.iter().map(|c| c.len()).sum();
    let mut bldr = Float64Builder::with_capacity(n);

    for chunk in &chunks {
        for geom in as_geo_geometries(chunk.as_ref())? {
            if let Some(val) = geom {
                bldr.append_value(val.signed_area());
            } else {
                bldr.append_null();
            }
        }
    }

    bldr.finish().into_arrow_robj()
}

/// @export
/// @rdname area
/// @family area
#[extendr]
fn ga_unsigned_area(x: Robj) -> extendr_api::Result<Robj> {
    let chunks = as_geometry_chunks(x)?;
    let n = chunks.iter().map(|c| c.len()).sum();
    let mut bldr = Float64Builder::with_capacity(n);

    for chunk in &chunks {
        for geom in as_geo_geometries(chunk.as_ref())? {
            if let Some(val) = geom {
                bldr.append_value(val.unsigned_area());
            } else {
                bldr.append_null();
            }
        }
    }

    bldr.finish().into_arrow_robj()
}

/// Chamberlain-Duquette spherical area
///
/// These functions use the Chamberlain-Duquette formula, which is suitable for
/// geographic coordinates on a sphere. They return area in square meters.
///
/// @param x a GeoArrow geometry array
/// @returns a double vector of area values in square meters
/// @export
/// @rdname area_cd
/// @family area
/// @references [ChamberlainDuquetteArea](https://docs.rs/geo/latest/geo/algorithm/chamberlain_duquette_area/trait.ChamberlainDuquetteArea.html)
/// @examplesIf requireNamespace("sf", quietly = TRUE) && requireNamespace("geoarrow", quietly = TRUE)
/// fp <- system.file("shape/nc.shp", package = "sf")
/// nc <- as.data.frame(read_shapefile(fp))
///
/// # square meters, not square degrees
/// head(as.vector(ga_unsigned_area_cd(nc$geometry)))
/// head(as.vector(ga_signed_area_cd(nc$geometry)))
#[extendr]
fn ga_signed_area_cd(x: Robj) -> extendr_api::Result<Robj> {
    let chunks = as_geometry_chunks(x)?;
    let n = chunks.iter().map(|c| c.len()).sum();
    let mut bldr = Float64Builder::with_capacity(n);

    for chunk in &chunks {
        for geom in as_geo_geometries(chunk.as_ref())? {
            if let Some(val) = geom {
                bldr.append_value(val.chamberlain_duquette_signed_area());
            } else {
                bldr.append_null();
            }
        }
    }

    bldr.finish().into_arrow_robj()
}

/// @export
/// @rdname area_cd
/// @family area
#[extendr]
fn ga_unsigned_area_cd(x: Robj) -> extendr_api::Result<Robj> {
    let chunks = as_geometry_chunks(x)?;
    let n = chunks.iter().map(|c| c.len()).sum();
    let mut bldr = Float64Builder::with_capacity(n);

    for chunk in &chunks {
        for geom in as_geo_geometries(chunk.as_ref())? {
            if let Some(val) = geom {
                bldr.append_value(val.chamberlain_duquette_unsigned_area());
            } else {
                bldr.append_null();
            }
        }
    }

    bldr.finish().into_arrow_robj()
}

/// Geodesic area and perimeter
///
/// These functions use the geodesic formula for computing area and perimeter on
/// an ellipsoidal model of the earth. They return area in square meters and
/// perimeter in meters.
///
/// @details
/// `geo` assumes Simple Features winding, so it expects an exterior ring to run
/// counter-clockwise. `ga_unsigned_area_geodesic()` reads a clockwise ring as
/// the rest of the earth instead and returns a value near 5.1e14. For
/// shapefiles, which wind clockwise, orient with [ga_orient()] first or read
/// the sign off `ga_signed_area_geodesic()`, which does not have this problem.
///
/// @param x a GeoArrow geometry array
/// @returns a double vector of area values in square meters, or perimeter values in meters
/// @export
/// @rdname area_geodesic
/// @family area
/// @references [GeodesicArea](https://docs.rs/geo/latest/geo/algorithm/geodesic_area/trait.GeodesicArea.html)
/// @examplesIf requireNamespace("sf", quietly = TRUE) && requireNamespace("geoarrow", quietly = TRUE)
/// fp <- system.file("shape/nc.shp", package = "sf")
/// nc <- as.data.frame(read_shapefile(fp))
///
/// head(as.vector(ga_signed_area_geodesic(nc$geometry)))
/// head(as.vector(ga_perimeter_geodesic(nc$geometry)))
///
/// # orient nc, which winds clockwise, before taking the unsigned area
/// ccw <- ga_orient(nc$geometry, "default")
/// head(as.vector(ga_unsigned_area_geodesic(ccw)))
#[extendr]
fn ga_signed_area_geodesic(x: Robj) -> extendr_api::Result<Robj> {
    let chunks = as_geometry_chunks(x)?;
    let n = chunks.iter().map(|c| c.len()).sum();
    let mut bldr = Float64Builder::with_capacity(n);

    for chunk in &chunks {
        for geom in as_geo_geometries(chunk.as_ref())? {
            if let Some(val) = geom {
                bldr.append_value(val.geodesic_area_signed());
            } else {
                bldr.append_null();
            }
        }
    }

    bldr.finish().into_arrow_robj()
}

/// @export
/// @rdname area_geodesic
/// @family area
#[extendr]
fn ga_unsigned_area_geodesic(x: Robj) -> extendr_api::Result<Robj> {
    let chunks = as_geometry_chunks(x)?;
    let n = chunks.iter().map(|c| c.len()).sum();
    let mut bldr = Float64Builder::with_capacity(n);

    for chunk in &chunks {
        for geom in as_geo_geometries(chunk.as_ref())? {
            if let Some(val) = geom {
                bldr.append_value(val.geodesic_area_unsigned());
            } else {
                bldr.append_null();
            }
        }
    }

    bldr.finish().into_arrow_robj()
}

/// @export
/// @rdname area_geodesic
/// @family area
#[extendr]
fn ga_perimeter_geodesic(x: Robj) -> extendr_api::Result<Robj> {
    let chunks = as_geometry_chunks(x)?;
    let n = chunks.iter().map(|c| c.len()).sum();
    let mut bldr = Float64Builder::with_capacity(n);

    for chunk in &chunks {
        for geom in as_geo_geometries(chunk.as_ref())? {
            if let Some(val) = geom {
                bldr.append_value(val.geodesic_perimeter());
            } else {
                bldr.append_null();
            }
        }
    }

    bldr.finish().into_arrow_robj()
}

/// Planar length of a polygon's exterior and interior rings.
trait PerimeterEuclidean {
    fn perimeter_euclidean(&self) -> f64;
}

impl PerimeterEuclidean for Polygon<f64> {
    fn perimeter_euclidean(&self) -> f64 {
        Euclidean.length(self.exterior())
            + self
                .interiors()
                .iter()
                .map(|r| Euclidean.length(r))
                .sum::<f64>()
    }
}

/// Anything without an area has no perimeter.
impl PerimeterEuclidean for Geometry<f64> {
    fn perimeter_euclidean(&self) -> f64 {
        match self {
            Geometry::Polygon(p) => p.perimeter_euclidean(),
            Geometry::MultiPolygon(mp) => mp.0.iter().map(|p| p.perimeter_euclidean()).sum(),
            Geometry::Rect(r) => r.to_polygon().perimeter_euclidean(),
            Geometry::Triangle(t) => t.to_polygon().perimeter_euclidean(),
            Geometry::GeometryCollection(gc) => gc.0.iter().map(|g| g.perimeter_euclidean()).sum(),
            _ => 0.0,
        }
    }
}

/// Planar perimeter of polygons
///
/// Sums the length of every ring, including interior rings, in the units of the
/// coordinate system. `ga_perimeter_euclidean()` returns 0 for points and
/// lines, which have no area.
///
/// @details
/// Use `ga_perimeter_geodesic()` for geographic coordinates.
/// `ga_perimeter_euclidean()` returns `NA` for a null geometry.
///
/// @param x a GeoArrow geometry array
/// @returns a double array of the same length as `x`
/// @export
/// @family area
/// @references [Length](https://docs.rs/geo/latest/geo/algorithm/line_measures/trait.Length.html)
/// @examplesIf requireNamespace("sf", quietly = TRUE) && requireNamespace("geoarrow", quietly = TRUE)
/// sq <- geoarrow::as_geoarrow_array(sf::st_sfc(
///   sf::st_polygon(list(rbind(c(0, 0), c(2, 0), c(2, 2), c(0, 2), c(0, 0))))
/// ))
///
/// as.vector(ga_perimeter_euclidean(sq))
#[extendr]
fn ga_perimeter_euclidean(x: Robj) -> extendr_api::Result<Robj> {
    let chunks = as_geometry_chunks(x)?;
    let n = chunks.iter().map(|c| c.len()).sum();
    let mut bldr = Float64Builder::with_capacity(n);

    for chunk in &chunks {
        for geom in as_geo_geometries(chunk.as_ref())? {
            match geom {
                Some(g) => bldr.append_value(g.perimeter_euclidean()),
                None => bldr.append_null(),
            }
        }
    }

    bldr.finish().into_arrow_robj()
}

extendr_module! {
    mod area;
    fn ga_signed_area;
    fn ga_unsigned_area;
    fn ga_signed_area_cd;
    fn ga_unsigned_area_cd;
    fn ga_signed_area_geodesic;
    fn ga_unsigned_area_geodesic;
    fn ga_perimeter_geodesic;
    fn ga_perimeter_euclidean;
}
