use extendr_api::prelude::*;

use crate::as_geometry_chunks;

/// Reproject geometries to another coordinate reference system
///
/// A placeholder for reprojection without PROJ. It errors until the work
/// below lands.
///
/// @details
/// The goal is to reproject coordinates between EPSG coordinate reference
/// systems with no PROJ dependency, matching PROJ to floating point error,
/// and to error whenever geoarrowrs cannot promise that. "Matching PROJ"
/// means PROJ as installed by default, with no grid files and no network
/// downloads.
///
/// **How a conversion works.** A coordinate reference system combines a
/// datum (an ellipsoid that models the shape of the Earth, pinned to the
/// Earth at a known place), a prime meridian (where longitude 0 is), and
/// usually a projection (the maths that flattens longitude and latitude to x
/// and y). Converting between two of them takes up to two steps: a datum
/// shift, which moves coordinates from one datum to the other, then the
/// projection. The projection is pure maths. The datum shift uses published
/// parameters, either 3 or 7 numbers (a Helmert shift, `+towgs84=` in a PROJ
/// string) or a grid file of measured corrections. EPSG often publishes
/// several shifts for one pair of datums, and PROJ picks the most accurate one
/// it can run.
///
/// **The engine.** proj4rs (<https://github.com/3liz/proj4rs>) is a pure Rust
/// port of proj4js under active development. It builds a projection from a
/// PROJ string, or from an EPSG code with its `crs-definitions` feature, but
/// not from WKT or PROJJSON, and it takes geographic coordinates in radians.
/// Fixes to proj4rs itself go upstream as pull requests.
///
/// **What we measured.** On 2026-10-25 we built every definition in
/// crs-definitions 0.6.0 with proj4rs at d9a6284 (0.3.0, unreleased), pushed
/// one point half a degree from each projection's origin through it from
/// WGS 84, and compared the result with PROJ through `sf::sf_project()`, using
/// sf's bundled PROJ data with no grids and the network off.
///
/// - proj4rs builds 5,942 of the 6,182 definitions. Of the 240 it cannot
///   build, 205 use `+datum=NAD27`, whose grids PROJ treats as optional
///   (`@conus`) but proj4rs requires, 33 need New Zealand's
///   `nzgd2kgrid0005.gsb`, and 2 use `labrd` and `nzmg`, which proj4rs does
///   not implement.
/// - Given the same PROJ string, proj4rs and PROJ agree to floating point,
///   except for 59 definitions on named datums (`hermannskogel`, `OSGB36`,
///   `ire65`, and others). Those differ by up to 592 m although proj4rs holds
///   the same `towgs84` values, and we have not found the cause.
/// - EPSG:4326 to EPSG:27572 agrees with PROJ to 2.5e-8 m at five French
///   cities, because the shift in the PROJ string is the one PROJ picks.
/// - Against PROJ given the EPSG codes, as `sf::st_transform()` does, proj4rs
///   agrees for 69.0% of definitions within 1 micrometre, 71.5% within 1 mm,
///   and 75.6% within 1 m. The datum shift explains every larger difference:
///   1,299 crs-definitions strings carry a different shift from the one PROJ
///   picks, and 163 carry none where PROJ applies one.
///
/// **The rules.** `ga_transform()` returns a result only when it matches PROJ
/// to floating point, and errors with the reason otherwise.
///
/// 1. When both systems share a datum, only the projection runs, which is
///    exact.
/// 2. When one side is WGS 84 and the shift of the other equals the one PROJ
///    picks, the result is exact everywhere, since identical parameters give
///    identical results.
/// 3. Anything else is an error: a grid applies, the projection has no
///    implementation, the shift differs from PROJ's (including datums where
///    PROJ picks a shift by region), or the two sides use different datums
///    other than WGS 84, which PROJ may convert between directly rather than
///    through WGS 84.
///
/// **Next steps.**
///
/// 1. Write a development script that uses PROJ through sf to build our own
///    EPSG table, recording for each code its projection, the shift to WGS 84
///    that PROJ picks without grids, and whether the code is exact or why not.
///    Decide exactness by comparing parameters rather than test points. PROJ
///    never runs at install or run time.
/// 2. Rerun the comparison against that table, and publish every code that
///    errors, with its reason, in the package docs.
/// 3. Send proj4rs pull requests that treat `@` grids as optional, fix the
///    named datum discrepancy once we find it, and add `labrd` and `nzmg`.
/// 4. Read the source system from the metadata of `x`: the EPSG `id` of its
///    PROJJSON through projjson-rs, or an `AUTHORITY:CODE`. Error for a system
///    the table does not hold, such as raw WKT.
/// 5. Apply the rules, run proj4rs, convert degrees to radians and back, and
///    write the target system to the output as PROJJSON.
/// 6. Test every allowed pair against `sf::st_transform()` row by row, and
///    every refused pair for its error.
///
/// @param x a GeoArrow geometry array
/// @param crs the coordinate reference system to reproject to, such as
///   `"EPSG:27572"`
/// @returns nothing yet; it always errors
/// @noRd
#[extendr]
fn ga_transform(x: Robj, crs: &str) -> extendr_api::Result<Robj> {
    as_geometry_chunks(x)?;
    Err(Error::Other(format!(
        "`ga_transform()` is not implemented yet, so it cannot reproject to {crs}"
    )))
}

extendr_module! {
    mod transform;
    fn ga_transform;
}
