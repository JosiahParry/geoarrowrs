use arrow::array::Float64Builder;
use arrow_extendr::IntoArrowRobj;
use extendr_api::prelude::*;
use geo::{
    Distance, Euclidean, Geodesic, HausdorffDistance, Haversine, Rhumb, VincentyDistance,
    line_measures::FrechetDistance,
};
use geo_traits::to_geo::{ToGeoGeometry, ToGeoLineString, ToGeoPoint};
use geoarrow::array::{
    GeoArrowArray, GeoArrowArrayAccessor, GeometryArray, LineStringArray, PointArray,
};

use crate::{as_geometry_chunks, as_linestring_chunks, as_point_chunks};

// TODO: use rayon with min chunk size of 4096
#[extendr]
fn dist_euclidean_pairwise(origin: Robj, dest: Robj) -> extendr_api::Result<Robj> {
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
fn dist_haversine_pairwise(origin: Robj, dest: Robj) -> extendr_api::Result<Robj> {
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
fn dist_geodesic_pairwise(origin: Robj, dest: Robj) -> extendr_api::Result<Robj> {
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
fn dist_rhumb_pairwise(origin: Robj, dest: Robj) -> extendr_api::Result<Robj> {
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

#[extendr]
fn dist_hausdorff_pairwise(origin: Robj, dest: Robj) -> extendr_api::Result<Robj> {
    let origin_chunks = as_geometry_chunks(origin)?;
    let dest_chunks = as_geometry_chunks(dest)?;
    let n = origin_chunks.iter().map(|c| c.len()).sum();
    let mut bldr = Float64Builder::with_capacity(n);

    for (orig, dst) in origin_chunks.iter().zip(dest_chunks.iter()) {
        let orig_arr = orig
            .as_any()
            .downcast_ref::<GeometryArray>()
            .ok_or_else(|| Error::Other("expected GeometryArray".to_string()))?;
        let dst_arr = dst
            .as_any()
            .downcast_ref::<GeometryArray>()
            .ok_or_else(|| Error::Other("expected GeometryArray".to_string()))?;
        dist_hausdorff_impl(&mut bldr, orig_arr, dst_arr);
    }

    bldr.finish().into_arrow_robj()
}

fn dist_hausdorff_impl(bldr: &mut Float64Builder, origin: &GeometryArray, dest: &GeometryArray) {
    for (xi, yi) in origin.iter().zip(dest.iter()) {
        if let (Some(Ok(x)), Some(Ok(y))) = (xi, yi) {
            let dist = x.to_geometry().hausdorff_distance(&y.to_geometry());
            bldr.append_value(dist);
        } else {
            bldr.append_null();
        }
    }
}

#[extendr]
fn dist_vincenty_pairwise(origin: Robj, dest: Robj) -> extendr_api::Result<Robj> {
    let origin_chunks = as_point_chunks(origin)?;
    let dest_chunks = as_point_chunks(dest)?;
    let n = origin_chunks.iter().map(|c| c.len()).sum();
    let mut bldr = Float64Builder::with_capacity(n);

    for (orig, dst) in origin_chunks.iter().zip(dest_chunks.iter()) {
        dist_vincenty_impl(&mut bldr, orig, dst);
    }

    bldr.finish().into_arrow_robj()
}

fn dist_vincenty_impl(bldr: &mut Float64Builder, origin: &PointArray, dest: &PointArray) {
    for (xi, yi) in origin.iter().zip(dest.iter()) {
        if let (Some(Ok(x)), Some(Ok(y))) = (xi, yi) {
            match x.to_point().vincenty_distance(&y.to_point()) {
                Ok(dist) => bldr.append_value(dist),
                Err(_) => bldr.append_null(),
            }
        } else {
            bldr.append_null();
        }
    }
}

#[extendr]
fn dist_frechet_pairwise(origin: Robj, dest: Robj) -> extendr_api::Result<Robj> {
    let origin_chunks = as_linestring_chunks(origin)?;
    let dest_chunks = as_linestring_chunks(dest)?;
    let n = origin_chunks.iter().map(|c| c.len()).sum();
    let mut bldr = Float64Builder::with_capacity(n);

    for (orig, dst) in origin_chunks.iter().zip(dest_chunks.iter()) {
        dist_frechet_impl(&mut bldr, orig, dst);
    }

    bldr.finish().into_arrow_robj()
}

fn dist_frechet_impl(bldr: &mut Float64Builder, origin: &LineStringArray, dest: &LineStringArray) {
    for (xi, yi) in origin.iter().zip(dest.iter()) {
        if let (Some(Ok(x)), Some(Ok(y))) = (xi, yi) {
            let dist = Euclidean.frechet_distance(&x.to_line_string(), &y.to_line_string());
            bldr.append_value(dist);
        } else {
            bldr.append_null();
        }
    }
}

extendr_module! {
    mod distance;
    fn dist_euclidean_pairwise;
    fn dist_haversine_pairwise;
    fn dist_geodesic_pairwise;
    fn dist_rhumb_pairwise;
    fn dist_hausdorff_pairwise;
    fn dist_vincenty_pairwise;
    fn dist_frechet_pairwise;
}
