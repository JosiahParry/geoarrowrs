use arrow_extendr::IntoArrowRobj;
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

use crate::{as_point_chunks, check_recycle_len, try_float_array};

/// Check that two point arrays being walked in lockstep have the same length.
fn check_pair_len(n_start: usize, n_end: usize) -> extendr_api::Result<()> {
    if n_start != n_end {
        return Err(Error::Other(format!(
            "`start` and `end` must be the same length, got {n_start} and {n_end}"
        )));
    }
    Ok(())
}

/// Interpolate a point at a given distance between two points
///
/// Returns the point located at `distance` along the path from `start` to `end`.
/// The metric determines how distance is measured.
///
/// @param start a GeoArrow point array of start points
/// @param end a GeoArrow point array of end points
/// @param distance a numeric vector of distances; length 1 or the same length as `start`
/// @param metric one of `"euclidean"`, `"haversine"`, `"geodesic"`, or `"rhumb"`
/// @returns a GeoArrow point array
/// @export
/// @rdname interpolate_between
/// @family interpolate
/// @references [InterpolatePoint](https://docs.rs/geo/latest/geo/algorithm/line_measures/trait.InterpolatePoint.html)
#[extendr]
fn ga_point_at_distance_between(
    start: Robj,
    end: Robj,
    distance: Robj,
    metric: &str,
) -> extendr_api::Result<Robj> {
    let start_chunks = as_point_chunks(start)?;
    let end_chunks = as_point_chunks(end)?;
    let distance = try_float_array(distance, "distance")?;

    let n: usize = start_chunks.iter().map(|c| c.len()).sum();
    check_pair_len(n, end_chunks.iter().map(|c| c.len()).sum())?;
    check_recycle_len(distance.len(), n, "distance")?;

    let metadata = start_chunks[0].data_type().metadata().clone();
    let mut bldr = PointBuilder::new(PointType::new(geoarrow::datatypes::Dimension::XY, metadata));

    macro_rules! dispatch {
        ($metric_val:expr) => {{
            let mut di = distance.iter().cycle();
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

    let res = bldr.finish();
    res.into_arrow_robj()
}

/// Interpolate a point at a given ratio between two points
///
/// Returns the point located at `ratio` of the way from `start` to `end`,
/// where 0.0 is the start and 1.0 is the end. The metric determines how
/// the interpolation is computed.
///
/// @param start a GeoArrow point array of start points
/// @param end a GeoArrow point array of end points
/// @param ratio a numeric vector of ratios between 0 and 1; length 1 or the same length as `start`
/// @param metric one of `"euclidean"`, `"haversine"`, `"geodesic"`, or `"rhumb"`
/// @returns a GeoArrow point array
/// @export
/// @rdname interpolate_between
/// @family interpolate
/// @references [InterpolatePoint](https://docs.rs/geo/latest/geo/algorithm/line_measures/trait.InterpolatePoint.html)
#[extendr]
fn ga_point_at_ratio_between(
    start: Robj,
    end: Robj,
    ratio: Robj,
    metric: &str,
) -> extendr_api::Result<Robj> {
    let start_chunks = as_point_chunks(start)?;
    let end_chunks = as_point_chunks(end)?;
    let ratio = try_float_array(ratio, "ratio")?;

    let n: usize = start_chunks.iter().map(|c| c.len()).sum();
    check_pair_len(n, end_chunks.iter().map(|c| c.len()).sum())?;
    check_recycle_len(ratio.len(), n, "ratio")?;

    let metadata = start_chunks[0].data_type().metadata().clone();
    let mut bldr = PointBuilder::new(PointType::new(geoarrow::datatypes::Dimension::XY, metadata));

    macro_rules! dispatch {
        ($metric_val:expr) => {{
            let mut ri = ratio.iter().cycle();
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

    let res = bldr.finish();
    res.into_arrow_robj()
}

/// Generate points at regular intervals along the line between two points
///
/// Returns a multipoint array where each element contains all points spaced
/// at most `max_distance` apart along the path from `start` to `end`.
///
/// @param start a GeoArrow point array of start points
/// @param end a GeoArrow point array of end points
/// @param max_distance the maximum spacing between generated points; length 1 or the same length as `start`
/// @param include_ends whether to include the start and end points in the output
/// @param metric one of `"euclidean"`, `"haversine"`, `"geodesic"`, or `"rhumb"`
/// @returns a GeoArrow multipoint array
/// @export
/// @family interpolate
/// @references [InterpolatePoint](https://docs.rs/geo/latest/geo/algorithm/line_measures/trait.InterpolatePoint.html)
#[extendr]
fn ga_points_along_line(
    start: Robj,
    end: Robj,
    max_distance: Robj,
    include_ends: bool,
    metric: &str,
) -> extendr_api::Result<Robj> {
    let start_chunks = as_point_chunks(start)?;
    let end_chunks = as_point_chunks(end)?;
    let max_distance = try_float_array(max_distance, "max_distance")?;

    let n: usize = start_chunks.iter().map(|c| c.len()).sum();
    check_pair_len(n, end_chunks.iter().map(|c| c.len()).sum())?;
    check_recycle_len(max_distance.len(), n, "max_distance")?;

    let metadata = start_chunks[0].data_type().metadata().clone();
    let mut bldr = MultiPointBuilder::new(MultiPointType::new(
        geoarrow::datatypes::Dimension::XY,
        metadata,
    ));

    macro_rules! dispatch {
        ($metric_val:expr) => {{
            let mut mdi = max_distance.iter().cycle();
            for (sc, ec) in start_chunks.iter().zip(end_chunks.iter()) {
                for (si, ei) in sc.iter().zip(ec.iter()) {
                    let md = mdi.next().flatten();
                    if let (Some(Ok(s)), Some(Ok(e)), Some(md)) = (si, ei, md) {
                        let pts: MultiPoint<f64> = $metric_val
                            .points_along_line(s.to_point(), e.to_point(), md, include_ends)
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

    let res = bldr.finish();
    res.into_arrow_robj()
}

extendr_module! {
    mod interpolate_point;
    fn ga_point_at_distance_between;
    fn ga_point_at_ratio_between;
    fn ga_points_along_line;
}
