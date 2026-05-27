use crate::{as_point_chunks, try_float_array};
use arrow::array::Float64Array;
use arrow_extendr::IntoArrowRobj;
use extendr_api::prelude::*;
use geo::{Destination, Euclidean, Geodesic, Haversine, Rhumb};
use geo_traits::to_geo::ToGeoPoint;
use geoarrow::{
    array::{GeoArrowArray, GeoArrowArrayAccessor, PointArray, PointBuilder},
    datatypes::PointType,
};

#[extendr]
fn dest_rhumb(origin: Robj, bearing: Robj, distance: Robj) -> extendr_api::Result<Robj> {
    let origin = as_point_chunks(origin)?;
    let n = origin.iter().map(|c| c.len()).sum();
    let bearing = try_float_array(bearing, "bearing")?;
    let distance = try_float_array(distance, "distance")?;

    let dt = origin[0].data_type();
    let metadata = dt.metadata().clone();

    let mut bldr = PointBuilder::with_capacity(
        PointType::new(geoarrow::datatypes::Dimension::XY, metadata),
        n,
    );
    for chunk in origin {
        dest_rhumb_impl(&mut bldr, &chunk, &bearing, &distance);
    }

    bldr.finish().into_arrow_robj()
}

#[extendr]
fn dest_euclidean(origin: Robj, bearing: Robj, distance: Robj) -> extendr_api::Result<Robj> {
    let origin = as_point_chunks(origin)?;
    let n = origin.iter().map(|c| c.len()).sum();
    let bearing = try_float_array(bearing, "bearing")?;
    let distance = try_float_array(distance, "distance")?;

    let dt = origin[0].data_type();
    let metadata = dt.metadata().clone();

    let mut bldr = PointBuilder::with_capacity(
        PointType::new(geoarrow::datatypes::Dimension::XY, metadata),
        n,
    );
    for chunk in origin {
        dest_euclidean_impl(&mut bldr, &chunk, &bearing, &distance);
    }

    bldr.finish().into_arrow_robj()
}

#[extendr]
fn dest_haversine(origin: Robj, bearing: Robj, distance: Robj) -> extendr_api::Result<Robj> {
    let origin = as_point_chunks(origin)?;
    let n = origin.iter().map(|c| c.len()).sum();
    let bearing = try_float_array(bearing, "bearing")?;
    let distance = try_float_array(distance, "distance")?;

    let dt = origin[0].data_type();
    let metadata = dt.metadata().clone();

    let mut bldr = PointBuilder::with_capacity(
        PointType::new(geoarrow::datatypes::Dimension::XY, metadata),
        n,
    );
    for chunk in origin {
        dest_haversine_impl(&mut bldr, &chunk, &bearing, &distance);
    }

    bldr.finish().into_arrow_robj()
}

#[extendr]
fn dest_geodesic(origin: Robj, bearing: Robj, distance: Robj) -> extendr_api::Result<Robj> {
    let origin = as_point_chunks(origin)?;
    let n = origin.iter().map(|c| c.len()).sum();
    let bearing = try_float_array(bearing, "bearing")?;
    let distance = try_float_array(distance, "distance")?;

    let dt = origin[0].data_type();
    let metadata = dt.metadata().clone();

    let mut bldr = PointBuilder::with_capacity(
        PointType::new(geoarrow::datatypes::Dimension::XY, metadata),
        n,
    );
    for chunk in origin {
        dest_geodesic_impl(&mut bldr, &chunk, &bearing, &distance);
    }

    bldr.finish().into_arrow_robj()
}

fn dest_rhumb_impl(
    bldr: &mut PointBuilder,
    origin: &PointArray,
    bearing: &Float64Array,
    distance: &Float64Array,
) {
    for xi in origin
        .iter()
        .zip(bearing.iter().cycle())
        .zip(distance.iter().cycle())
    {
        let ((xi, bi), di) = xi;

        if let (Some(Ok(origin)), Some(bearing), Some(distance)) = (xi, bi, di) {
            let v = Rhumb.destination(origin.to_point(), bearing, distance);
            bldr.push_point(Some(&v));
        } else {
            bldr.push_null();
        }
    }
}

fn dest_euclidean_impl(
    bldr: &mut PointBuilder,
    origin: &PointArray,
    bearing: &Float64Array,
    distance: &Float64Array,
) {
    for xi in origin
        .iter()
        .zip(bearing.iter().cycle())
        .zip(distance.iter().cycle())
    {
        let ((xi, bi), di) = xi;

        if let (Some(Ok(origin)), Some(bearing), Some(distance)) = (xi, bi, di) {
            let v = Euclidean.destination(origin.to_point(), bearing, distance);
            bldr.push_point(Some(&v));
        } else {
            bldr.push_null();
        }
    }
}

fn dest_haversine_impl(
    bldr: &mut PointBuilder,
    origin: &PointArray,
    bearing: &Float64Array,
    distance: &Float64Array,
) {
    for xi in origin
        .iter()
        .zip(bearing.iter().cycle())
        .zip(distance.iter().cycle())
    {
        let ((xi, bi), di) = xi;

        if let (Some(Ok(origin)), Some(bearing), Some(distance)) = (xi, bi, di) {
            let v = Haversine.destination(origin.to_point(), bearing, distance);
            bldr.push_point(Some(&v));
        } else {
            bldr.push_null();
        }
    }
}

fn dest_geodesic_impl(
    bldr: &mut PointBuilder,
    origin: &PointArray,
    bearing: &Float64Array,
    distance: &Float64Array,
) {
    for xi in origin
        .iter()
        .zip(bearing.iter().cycle())
        .zip(distance.iter().cycle())
    {
        let ((xi, bi), di) = xi;

        if let (Some(Ok(origin)), Some(bearing), Some(distance)) = (xi, bi, di) {
            let v = Geodesic.destination(origin.to_point(), bearing, distance);
            bldr.push_point(Some(&v));
        } else {
            bldr.push_null();
        }
    }
}

extendr_module! {
    mod destination;
    fn dest_rhumb;
}
