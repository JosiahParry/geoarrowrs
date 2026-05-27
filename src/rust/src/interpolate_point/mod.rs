use extendr_api::prelude::*;
use geo::{
    Geodesic, MultiPoint, Rhumb,
    algorithm::{Euclidean, Haversine, InterpolatePoint},
};
use geo_traits::to_geo::ToGeoPoint;
use geoarrow::{
    array::{MultiPointBuilder, PointBuilder},
    datatypes::{MultiPointType, PointType},
};
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor};

use crate::{as_point_chunks, try_float_array};

#[extendr]
fn point_at_distance_between(
    start: Robj,
    end: Robj,
    distance: Robj,
    metric: &str,
) -> extendr_api::Result<()> {
    let start_chunks = as_point_chunks(start)?;
    let end_chunks = as_point_chunks(end)?;
    let distance = try_float_array(distance, "distance")?;

    let metadata = start_chunks[0].data_type().metadata().clone();
    let mut bldr = PointBuilder::new(PointType::new(geoarrow::datatypes::Dimension::XY, metadata));

    macro_rules! dispatch {
        ($metric_val:expr) => {{
            let mut di = distance.iter();
            for (sc, ec) in start_chunks.iter().zip(end_chunks.iter()) {
                for (si, ei) in sc.iter().zip(ec.iter()) {
                    let dv = di.next().flatten();
                    if let (Some(Ok(s)), Some(Ok(e)), Some(d)) = (si, ei, dv) {
                        let pt =
                            $metric_val.point_at_distance_between(s.to_point(), e.to_point(), d);
                        bldr.push_point(Some(&pt));
                    } else {
                        bldr.push_null();
                    }
                }
            }
        }};
    }

    match metric {
        "euclidean" => dispatch!(Euclidean),
        "haversine" => dispatch!(Haversine),
        "geodesic" => dispatch!(Geodesic),
        "rhumb" => dispatch!(Rhumb),
        _ => {
            return Err(Error::Other(
                "`metric` must be one of `euclidean`, `haversine`, `geodesic`, or `rhumb`"
                    .to_string(),
            ));
        }
    }

    Ok(())
}

#[extendr]
fn point_at_ratio_between(
    start: Robj,
    end: Robj,
    ratio: Robj,
    metric: &str,
) -> extendr_api::Result<()> {
    let start_chunks = as_point_chunks(start)?;
    let end_chunks = as_point_chunks(end)?;
    let ratio = try_float_array(ratio, "ratio")?;

    let metadata = start_chunks[0].data_type().metadata().clone();
    let mut bldr = PointBuilder::new(PointType::new(geoarrow::datatypes::Dimension::XY, metadata));

    macro_rules! dispatch {
        ($metric_val:expr) => {{
            let mut ri = ratio.iter();
            for (sc, ec) in start_chunks.iter().zip(end_chunks.iter()) {
                for (si, ei) in sc.iter().zip(ec.iter()) {
                    let rv = ri.next().flatten();
                    if let (Some(Ok(s)), Some(Ok(e)), Some(r)) = (si, ei, rv) {
                        let pt = $metric_val.point_at_ratio_between(s.to_point(), e.to_point(), r);
                        bldr.push_point(Some(&pt));
                    } else {
                        bldr.push_null();
                    }
                }
            }
        }};
    }

    match metric {
        "euclidean" => dispatch!(Euclidean),
        "haversine" => dispatch!(Haversine),
        "geodesic" => dispatch!(Geodesic),
        "rhumb" => dispatch!(Rhumb),
        _ => {
            return Err(Error::Other(
                "`metric` must be one of `euclidean`, `haversine`, `geodesic`, or `rhumb`"
                    .to_string(),
            ));
        }
    }

    Ok(())
}

#[extendr]
fn points_along_line(
    start: Robj,
    end: Robj,
    max_distance: f64,
    include_ends: bool,
    metric: &str,
) -> extendr_api::Result<()> {
    let start_chunks = as_point_chunks(start)?;
    let end_chunks = as_point_chunks(end)?;

    let metadata = start_chunks[0].data_type().metadata().clone();
    let mut bldr = MultiPointBuilder::new(MultiPointType::new(
        geoarrow::datatypes::Dimension::XY,
        metadata,
    ));

    macro_rules! dispatch {
        ($metric_val:expr) => {{
            for (sc, ec) in start_chunks.iter().zip(end_chunks.iter()) {
                for (si, ei) in sc.iter().zip(ec.iter()) {
                    if let (Some(Ok(s)), Some(Ok(e))) = (si, ei) {
                        let pts: MultiPoint<f64> = $metric_val
                            .points_along_line(
                                s.to_point(),
                                e.to_point(),
                                max_distance,
                                include_ends,
                            )
                            .collect();
                        bldr.push_multi_point(Some(&pts))
                            .map_err(|err| Error::Other(err.to_string()))?;
                    } else {
                        bldr.push_multi_point(None::<&MultiPoint<f64>>)
                            .map_err(|err| Error::Other(err.to_string()))?;
                    }
                }
            }
        }};
    }

    match metric {
        "euclidean" => dispatch!(Euclidean),
        "haversine" => dispatch!(Haversine),
        "geodesic" => dispatch!(Geodesic),
        "rhumb" => dispatch!(Rhumb),
        _ => {
            return Err(Error::Other(
                "`metric` must be one of `euclidean`, `haversine`, `geodesic`, or `rhumb`"
                    .to_string(),
            ));
        }
    }

    Ok(())
}
