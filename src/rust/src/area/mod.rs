use arrow::array::Float64Builder;
use arrow_extendr::IntoArrowRobj;
use extendr_api::prelude::*;
use geo::{Area, ChamberlainDuquetteArea, GeodesicArea};
use geoarrow_array::GeoArrowArray;

use crate::{as_geo_geometries, as_geometry_chunks};

/// Compute the signed and unsigned planar area of geometries
///
/// `signed_area()` returns positive values for counter-clockwise winding and
/// negative values for clockwise winding. `unsigned_area()` always returns a
/// non-negative value.
///
/// @param x a GeoArrow geometry array
/// @returns a double vector of area values in the units of the coordinate system
/// @export
/// @rdname area
/// @family area
/// @references [Area](https://docs.rs/geo/latest/geo/algorithm/area/trait.Area.html)
#[extendr]
fn signed_area(x: Robj) -> extendr_api::Result<Robj> {
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
/// @references [Area](https://docs.rs/geo/latest/geo/algorithm/area/trait.Area.html)
#[extendr]
fn unsigned_area(x: Robj) -> extendr_api::Result<Robj> {
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

/// Compute the signed and unsigned area using the Chamberlain-Duquette algorithm
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
#[extendr]
fn signed_area_cd(x: Robj) -> extendr_api::Result<Robj> {
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
/// @references [ChamberlainDuquetteArea](https://docs.rs/geo/latest/geo/algorithm/chamberlain_duquette_area/trait.ChamberlainDuquetteArea.html)
#[extendr]
fn unsigned_area_cd(x: Robj) -> extendr_api::Result<Robj> {
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

/// Compute the signed and unsigned geodesic area and perimeter of geometries
///
/// These functions use the geodesic formula for computing area and perimeter on
/// an ellipsoidal model of the earth. Results are in square meters for area and
/// meters for perimeter.
///
/// @param x a GeoArrow geometry array
/// @returns a double vector of area values in square meters, or perimeter values in meters
/// @export
/// @rdname area_geodesic
/// @family area
/// @references [GeodesicArea](https://docs.rs/geo/latest/geo/algorithm/geodesic_area/trait.GeodesicArea.html)
#[extendr]
fn signed_area_geodesic(x: Robj) -> extendr_api::Result<Robj> {
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
/// @references [GeodesicArea](https://docs.rs/geo/latest/geo/algorithm/geodesic_area/trait.GeodesicArea.html)
#[extendr]
fn unsigned_area_geodesic(x: Robj) -> extendr_api::Result<Robj> {
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
/// @references [GeodesicArea](https://docs.rs/geo/latest/geo/algorithm/geodesic_area/trait.GeodesicArea.html)
#[extendr]
fn perimeter_signed_geodesic(x: Robj) -> extendr_api::Result<Robj> {
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

/// @export
/// @rdname area_geodesic
/// @family area
/// @references [GeodesicArea](https://docs.rs/geo/latest/geo/algorithm/geodesic_area/trait.GeodesicArea.html)
#[extendr]
fn perimeter_unsigned_geodesic(x: Robj) -> extendr_api::Result<Robj> {
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
    fn signed_area;
    fn unsigned_area;
    fn signed_area_cd;
    fn unsigned_area_cd;
    fn signed_area_geodesic;
    fn unsigned_area_geodesic;
    fn perimeter_signed_geodesic;
    fn perimeter_unsigned_geodesic;
}
