use arrow::array::Float64Builder;
use arrow_extendr::IntoArrowRobj;
use extendr_api::prelude::*;
use geo::{Bearing, Euclidean, Geodesic, Haversine, Rhumb};
use geo_traits::to_geo::ToGeoPoint;
use geoarrow::array::{GeoArrowArray, GeoArrowArrayAccessor, PointArray};

use crate::as_point_chunks;

// TODO: use rayon with min chunk size of 4096
#[extendr]
fn bearing_euclidean(origin: Robj, dest: Robj) -> extendr_api::Result<Robj> {
    let origin_chunks = as_point_chunks(origin)?;
    let dest_chunks = as_point_chunks(dest)?;
    let n = origin_chunks.iter().map(|c| c.len()).sum();
    let mut bldr = Float64Builder::with_capacity(n);

    for (orig, dst) in origin_chunks.iter().zip(dest_chunks.iter()) {
        bearing_euclidean_impl(&mut bldr, orig, dst);
    }

    bldr.finish().into_arrow_robj()
}

fn bearing_euclidean_impl(bldr: &mut Float64Builder, origin: &PointArray, dest: &PointArray) {
    for (xi, yi) in origin.iter().zip(dest.iter()) {
        if let (Some(Ok(x)), Some(Ok(y))) = (xi, yi) {
            let dist = Euclidean.bearing(x.to_point(), y.to_point());
            bldr.append_value(dist);
        } else {
            bldr.append_null();
        }
    }
}

#[extendr]
fn bearing_haversine(origin: Robj, dest: Robj) -> extendr_api::Result<Robj> {
    let origin_chunks = as_point_chunks(origin)?;
    let dest_chunks = as_point_chunks(dest)?;
    let n = origin_chunks.iter().map(|c| c.len()).sum();
    let mut bldr = Float64Builder::with_capacity(n);

    for (orig, dst) in origin_chunks.iter().zip(dest_chunks.iter()) {
        bearing_haversine_impl(&mut bldr, orig, dst);
    }

    bldr.finish().into_arrow_robj()
}

fn bearing_haversine_impl(bldr: &mut Float64Builder, origin: &PointArray, dest: &PointArray) {
    for (xi, yi) in origin.iter().zip(dest.iter()) {
        if let (Some(Ok(x)), Some(Ok(y))) = (xi, yi) {
            let dist = Haversine.bearing(x.to_point(), y.to_point());
            bldr.append_value(dist);
        } else {
            bldr.append_null();
        }
    }
}

#[extendr]
fn bearing_geodesic(origin: Robj, dest: Robj) -> extendr_api::Result<Robj> {
    let origin_chunks = as_point_chunks(origin)?;
    let dest_chunks = as_point_chunks(dest)?;
    let n = origin_chunks.iter().map(|c| c.len()).sum();
    let mut bldr = Float64Builder::with_capacity(n);

    for (orig, dst) in origin_chunks.iter().zip(dest_chunks.iter()) {
        bearing_geodesic_impl(&mut bldr, orig, dst);
    }

    bldr.finish().into_arrow_robj()
}

fn bearing_geodesic_impl(bldr: &mut Float64Builder, origin: &PointArray, dest: &PointArray) {
    for (xi, yi) in origin.iter().zip(dest.iter()) {
        if let (Some(Ok(x)), Some(Ok(y))) = (xi, yi) {
            let dist = Geodesic.bearing(x.to_point(), y.to_point());
            bldr.append_value(dist);
        } else {
            bldr.append_null();
        }
    }
}

#[extendr]
fn bearing_rhumb(origin: Robj, dest: Robj) -> extendr_api::Result<Robj> {
    let origin_chunks = as_point_chunks(origin)?;
    let dest_chunks = as_point_chunks(dest)?;
    let n = origin_chunks.iter().map(|c| c.len()).sum();
    let mut bldr = Float64Builder::with_capacity(n);

    for (orig, dst) in origin_chunks.iter().zip(dest_chunks.iter()) {
        bearing_rhumb_impl(&mut bldr, orig, dst);
    }

    bldr.finish().into_arrow_robj()
}

fn bearing_rhumb_impl(bldr: &mut Float64Builder, origin: &PointArray, dest: &PointArray) {
    for (xi, yi) in origin.iter().zip(dest.iter()) {
        if let (Some(Ok(x)), Some(Ok(y))) = (xi, yi) {
            let dist = Rhumb.bearing(x.to_point(), y.to_point());
            bldr.append_value(dist);
        } else {
            bldr.append_null();
        }
    }
}

extendr_module! {
    mod bearing;
    fn bearing_euclidean;
    fn bearing_haversine;
    fn bearing_geodesic;
    fn bearing_rhumb;
}
