use arrow::array::Float64Builder;
use arrow_extendr::IntoArrowRobj;
use extendr_api::prelude::*;
use geo::{Area, ChamberlainDuquetteArea, GeodesicArea};
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
/// # nc rings wind clockwise, so the signed area is negative
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
/// geographic coordinates on a sphere. Results are in square meters.
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
/// an ellipsoidal model of the earth. Results are in square meters for area and
/// meters for perimeter.
///
/// @details
/// `geo` assumes Simple Features winding, so an exterior ring must run
/// counter-clockwise. A clockwise ring names the rest of the earth instead,
/// and `ga_unsigned_area_geodesic()` returns a value near 5.1e14. Shapefiles
/// wind clockwise, so orient with [ga_orient()] first or read the sign off
/// `ga_signed_area_geodesic()`, which is unaffected.
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
/// # nc winds clockwise, so orient before taking the unsigned area
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

extendr_module! {
    mod area;
    fn ga_signed_area;
    fn ga_unsigned_area;
    fn ga_signed_area_cd;
    fn ga_unsigned_area_cd;
    fn ga_signed_area_geodesic;
    fn ga_unsigned_area_geodesic;
    fn ga_perimeter_geodesic;
}
