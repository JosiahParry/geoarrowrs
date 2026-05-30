use arrow::array::Float64Builder;
use arrow_extendr::IntoArrowRobj;
use extendr_api::prelude::*;
use geo::{Area, ChamberlainDuquetteArea, GeodesicArea};
use geo_traits::to_geo::ToGeoGeometry;
use geoarrow::array::{GeoArrowArrayAccessor, GeometryArray};

use crate::as_geometry_chunks;

#[extendr]
fn signed_area(x: Robj) -> extendr_api::Result<Robj> {
    let chunks = as_geometry_chunks(x)?;
    let n = chunks.iter().map(|c| c.len()).sum();
    let mut bldr = Float64Builder::with_capacity(n);

    for chunk in &chunks {
        let arr = chunk
            .as_any()
            .downcast_ref::<GeometryArray>()
            .ok_or_else(|| Error::Other("expected GeometryArray".to_string()))?;
        for xi in arr.iter() {
            if let Some(Ok(val)) = xi {
                bldr.append_value(val.to_geometry().signed_area());
            } else {
                bldr.append_null();
            }
        }
    }

    bldr.finish().into_arrow_robj()
}

#[extendr]
fn unsigned_area(x: Robj) -> extendr_api::Result<Robj> {
    let chunks = as_geometry_chunks(x)?;
    let n = chunks.iter().map(|c| c.len()).sum();
    let mut bldr = Float64Builder::with_capacity(n);

    for chunk in &chunks {
        let arr = chunk
            .as_any()
            .downcast_ref::<GeometryArray>()
            .ok_or_else(|| Error::Other("expected GeometryArray".to_string()))?;
        for xi in arr.iter() {
            if let Some(Ok(val)) = xi {
                bldr.append_value(val.to_geometry().unsigned_area());
            } else {
                bldr.append_null();
            }
        }
    }

    bldr.finish().into_arrow_robj()
}

#[extendr]
fn signed_area_cd(x: Robj) -> extendr_api::Result<Robj> {
    let chunks = as_geometry_chunks(x)?;
    let n = chunks.iter().map(|c| c.len()).sum();
    let mut bldr = Float64Builder::with_capacity(n);

    for chunk in &chunks {
        let arr = chunk
            .as_any()
            .downcast_ref::<GeometryArray>()
            .ok_or_else(|| Error::Other("expected GeometryArray".to_string()))?;
        for xi in arr.iter() {
            if let Some(Ok(val)) = xi {
                bldr.append_value(val.to_geometry().chamberlain_duquette_signed_area());
            } else {
                bldr.append_null();
            }
        }
    }

    bldr.finish().into_arrow_robj()
}

#[extendr]
fn unsigned_area_cd(x: Robj) -> extendr_api::Result<Robj> {
    let chunks = as_geometry_chunks(x)?;
    let n = chunks.iter().map(|c| c.len()).sum();
    let mut bldr = Float64Builder::with_capacity(n);

    for chunk in &chunks {
        let arr = chunk
            .as_any()
            .downcast_ref::<GeometryArray>()
            .ok_or_else(|| Error::Other("expected GeometryArray".to_string()))?;
        for xi in arr.iter() {
            if let Some(Ok(val)) = xi {
                bldr.append_value(val.to_geometry().chamberlain_duquette_unsigned_area());
            } else {
                bldr.append_null();
            }
        }
    }

    bldr.finish().into_arrow_robj()
}

#[extendr]
fn signed_area_geodesic(x: Robj) -> extendr_api::Result<Robj> {
    let chunks = as_geometry_chunks(x)?;
    let n = chunks.iter().map(|c| c.len()).sum();
    let mut bldr = Float64Builder::with_capacity(n);

    for chunk in &chunks {
        let arr = chunk
            .as_any()
            .downcast_ref::<GeometryArray>()
            .ok_or_else(|| Error::Other("expected GeometryArray".to_string()))?;
        for xi in arr.iter() {
            if let Some(Ok(val)) = xi {
                bldr.append_value(val.to_geometry().geodesic_area_signed());
            } else {
                bldr.append_null();
            }
        }
    }

    bldr.finish().into_arrow_robj()
}

#[extendr]
fn unsigned_area_geodesic(x: Robj) -> extendr_api::Result<Robj> {
    let chunks = as_geometry_chunks(x)?;
    let n = chunks.iter().map(|c| c.len()).sum();
    let mut bldr = Float64Builder::with_capacity(n);

    for chunk in &chunks {
        let arr = chunk
            .as_any()
            .downcast_ref::<GeometryArray>()
            .ok_or_else(|| Error::Other("expected GeometryArray".to_string()))?;
        for xi in arr.iter() {
            if let Some(Ok(val)) = xi {
                bldr.append_value(val.to_geometry().geodesic_area_unsigned());
            } else {
                bldr.append_null();
            }
        }
    }

    bldr.finish().into_arrow_robj()
}

#[extendr]
fn perimeter_signed_geodesic(x: Robj) -> extendr_api::Result<Robj> {
    let chunks = as_geometry_chunks(x)?;
    let n = chunks.iter().map(|c| c.len()).sum();
    let mut bldr = Float64Builder::with_capacity(n);

    for chunk in &chunks {
        let arr = chunk
            .as_any()
            .downcast_ref::<GeometryArray>()
            .ok_or_else(|| Error::Other("expected GeometryArray".to_string()))?;
        for xi in arr.iter() {
            if let Some(Ok(val)) = xi {
                bldr.append_value(val.to_geometry().geodesic_perimeter());
            } else {
                bldr.append_null();
            }
        }
    }

    bldr.finish().into_arrow_robj()
}

#[extendr]
fn perimeter_unsigned_geodesic(x: Robj) -> extendr_api::Result<Robj> {
    let chunks = as_geometry_chunks(x)?;
    let n = chunks.iter().map(|c| c.len()).sum();
    let mut bldr = Float64Builder::with_capacity(n);

    for chunk in &chunks {
        let arr = chunk
            .as_any()
            .downcast_ref::<GeometryArray>()
            .ok_or_else(|| Error::Other("expected GeometryArray".to_string()))?;
        for xi in arr.iter() {
            if let Some(Ok(val)) = xi {
                bldr.append_value(val.to_geometry().geodesic_perimeter());
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
