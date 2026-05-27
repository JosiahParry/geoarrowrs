use arrow::array::Float64Builder;
use arrow_extendr::IntoArrowRobj;
use extendr_api::prelude::*;
use geo::{Distance, Euclidean, Geodesic, Haversine, Rhumb};
use geo_traits::to_geo::ToGeoPoint;
use geoarrow::array::{GeoArrowArray, GeoArrowArrayAccessor, PointArray};

use crate::as_point_chunks;

// TODO: use rayon with min chunk size of 4096
#[extendr]
fn dist_euclidean(origin: Robj, dest: Robj) -> extendr_api::Result<Robj> {
    let origin_chunks = as_point_chunks(origin)?;
    let dest_chunks = as_point_chunks(dest)?;
    let n = origin_chunks.iter().map(|c| c.len()).sum();
    let mut bldr = Float64Builder::with_capacity(n);

    for (orig, dst) in origin_chunks.iter().zip(dest_chunks.iter()) {
        dist_euclidean_impl(&mut bldr, orig, dst);
    }

    bldr.finish().into_arrow_robj()
}

fn dist_euclidean_impl(bldr: &mut Float64Builder, origin: &PointArray, dest: &PointArray) {
    for (xi, yi) in origin.iter().zip(dest.iter()) {
        if let (Some(Ok(x)), Some(Ok(y))) = (xi, yi) {
            let dist = Euclidean.distance(x.to_point(), y.to_point());
            bldr.append_value(dist);
        } else {
            bldr.append_null();
        }
    }
}

#[extendr]
fn dist_haversine(origin: Robj, dest: Robj) -> extendr_api::Result<Robj> {
    let origin_chunks = as_point_chunks(origin)?;
    let dest_chunks = as_point_chunks(dest)?;
    let n = origin_chunks.iter().map(|c| c.len()).sum();
    let mut bldr = Float64Builder::with_capacity(n);

    for (orig, dst) in origin_chunks.iter().zip(dest_chunks.iter()) {
        dist_haversine_impl(&mut bldr, orig, dst);
    }

    bldr.finish().into_arrow_robj()
}

fn dist_haversine_impl(bldr: &mut Float64Builder, origin: &PointArray, dest: &PointArray) {
    for (xi, yi) in origin.iter().zip(dest.iter()) {
        if let (Some(Ok(x)), Some(Ok(y))) = (xi, yi) {
            let dist = Haversine.distance(x.to_point(), y.to_point());
            bldr.append_value(dist);
        } else {
            bldr.append_null();
        }
    }
}

#[extendr]
fn dist_geodesic(origin: Robj, dest: Robj) -> extendr_api::Result<Robj> {
    let origin_chunks = as_point_chunks(origin)?;
    let dest_chunks = as_point_chunks(dest)?;
    let n = origin_chunks.iter().map(|c| c.len()).sum();
    let mut bldr = Float64Builder::with_capacity(n);

    for (orig, dst) in origin_chunks.iter().zip(dest_chunks.iter()) {
        dist_geodesic_impl(&mut bldr, orig, dst);
    }

    bldr.finish().into_arrow_robj()
}

fn dist_geodesic_impl(bldr: &mut Float64Builder, origin: &PointArray, dest: &PointArray) {
    for (xi, yi) in origin.iter().zip(dest.iter()) {
        if let (Some(Ok(x)), Some(Ok(y))) = (xi, yi) {
            let dist = Geodesic.distance(x.to_point(), y.to_point());
            bldr.append_value(dist);
        } else {
            bldr.append_null();
        }
    }
}

#[extendr]
fn dist_rhumb(origin: Robj, dest: Robj) -> extendr_api::Result<Robj> {
    let origin_chunks = as_point_chunks(origin)?;
    let dest_chunks = as_point_chunks(dest)?;
    let n = origin_chunks.iter().map(|c| c.len()).sum();
    let mut bldr = Float64Builder::with_capacity(n);

    for (orig, dst) in origin_chunks.iter().zip(dest_chunks.iter()) {
        dist_rhumb_impl(&mut bldr, orig, dst);
    }

    bldr.finish().into_arrow_robj()
}

fn dist_rhumb_impl(bldr: &mut Float64Builder, origin: &PointArray, dest: &PointArray) {
    for (xi, yi) in origin.iter().zip(dest.iter()) {
        if let (Some(Ok(x)), Some(Ok(y))) = (xi, yi) {
            let dist = Rhumb.distance(x.to_point(), y.to_point());
            bldr.append_value(dist);
        } else {
            bldr.append_null();
        }
    }
}

extendr_module! {
    mod distance;
    fn dist_euclidean;
    fn dist_haversine;
    fn dist_geodesic;
    fn dist_rhumb;
}
