use std::sync::Arc;

use extendr_api::prelude::*;
use geoarrow::datatypes::{Crs, CrsType, Metadata};
use projjson::ProjJson;
use serde_json::Value;

use crate::as_geometry_chunks;

/// GeoArrow metadata for a `crs` argument: a valid PROJJSON CRS typed `projjson`, `AUTHORITY:CODE` typed `authority_code`, and anything else untyped as given.
pub(crate) fn crs_metadata(crs: Option<String>) -> Arc<Metadata> {
    let Some(value) = crs else {
        return Arc::new(Metadata::default());
    };
    let projjson = serde_json::from_str::<Value>(&value)
        .ok()
        .filter(|json| matches!(ProjJson::try_from(json), Ok(ProjJson::Crs(_))));
    let word = |s: &str| !s.is_empty() && s.chars().all(|c| c.is_ascii_alphanumeric() || c == '_');
    let is_code = value
        .split_once(':')
        .is_some_and(|(authority, code)| word(authority) && word(code));
    let crs = match projjson {
        Some(json) => Crs::from_projjson(json),
        None if is_code => Crs::from_authority_code(value),
        None => Crs::from_unknown_crs_type(value),
    };
    Arc::new(Metadata::new(crs, None))
}

/// The CRS of a GeoArrow array read as PROJJSON, or `None` when it has none or one that is not a valid PROJJSON CRS.
pub(crate) fn projjson_crs(crs: &Crs) -> Option<projjson::crs::Crs> {
    let json = crs
        .crs_value()
        .filter(|_| matches!(crs.crs_type(), None | Some(CrsType::Projjson)))?;
    match ProjJson::try_from(json) {
        Ok(ProjJson::Crs(c)) => Some(c),
        _ => None,
    }
}

/// Check whether geometries are in longitude and latitude
///
/// Reports whether the coordinate reference system of `x` is geographic, with
/// coordinates in longitude and latitude rather than on a plane.
///
/// @details
/// `ga_is_longlat()` reads the PROJJSON CRS in the GeoArrow metadata of `x` and
/// decides as GDAL's `IsGeographic()` does, which [sf::st_is_longlat()] calls:
/// `TRUE` for a geographic CRS on its own, as the source of a bound CRS, or as
/// the first component of a compound CRS. It returns `NA` when `x` has no CRS,
/// and when its CRS is not PROJJSON, such as an authority code or WKT, because
/// reading those takes a CRS database that geoarrowrs does not ship.
///
/// @param x a GeoArrow geometry array
/// @returns `TRUE`, `FALSE`, or `NA`, once for the whole array
/// @export
/// @family crs
/// @examplesIf requireNamespace("geoarrow", quietly = TRUE) && requireNamespace("sf", quietly = TRUE)
/// raleigh <- sf::st_sfc(sf::st_point(c(-78.64, 35.78)), crs = 4326)
/// ga_is_longlat(geoarrow::as_geoarrow_array(raleigh))
///
/// # North Carolina's state plane, in metres
/// ga_is_longlat(geoarrow::as_geoarrow_array(sf::st_transform(raleigh, 32119)))
#[extendr]
fn ga_is_longlat(x: Robj) -> extendr_api::Result<Rbool> {
    let chunks = as_geometry_chunks(x)?;
    let geographic = chunks
        .first()
        .and_then(|chunk| projjson_crs(chunk.data_type().metadata().crs()))
        .map(|crs| crs.is_geographic());
    Ok(Rbool::from(geographic))
}

extendr_module! {
    mod crs;
    fn ga_is_longlat;
}
