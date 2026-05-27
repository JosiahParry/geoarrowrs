use extendr_api::prelude::*;
use geo::{
    Geodesic, Rhumb,
    algorithm::{Euclidean, Haversine, InterpolateLine},
};
use geo_traits::to_geo::ToGeoLineString;
use geoarrow::{array::PointBuilder, datatypes::PointType};
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor};

use crate::{as_linestring_chunks, try_float_array};

// I want signature:
// interpolate_point <- function(line, value, measure = c("ratio", "distance"), from = c("start", "end")) {
//   # then dispatch based on start vs. end
// }
// interpolate_line_ratio <- function(line, ratio, from = c("start", "end")) {}
// interpolate_line_distance <- function(line, ratio, from = c("start", "end")) {}

enum WhereFrom {
    Start,
    End,
}

impl TryFrom<&Robj> for WhereFrom {
    type Error = Error;

    fn try_from(robj: &Robj) -> Result<Self, Self::Error> {
        let s = Strings::try_from(robj)?;
        if s.len() > 1 {
            return Err(Error::Other(
                "Expected `from` to be a scalar string".to_string(),
            ));
        }

        let val = s.elt(0);
        if val.is_na() {
            return Err(Error::Other(
                "Expected `from` to be a non-missing scalar string".to_string(),
            ));
        }

        match val.as_ref() {
            "start" => Ok(Self::Start),
            "end" => Ok(Self::End),
            _ => Err(Error::Other(
                "Expected `from` to be one of `start` or `end`".to_string(),
            )),
        }
    }
}

impl TryFrom<Robj> for WhereFrom {
    type Error = Error;

    fn try_from(value: Robj) -> Result<Self, Self::Error> {
        Self::try_from(&value)
    }
}

impl From<WhereFrom> for Strings {
    fn from(value: WhereFrom) -> Self {
        match value {
            WhereFrom::Start => Strings::from_values(["start"]),
            WhereFrom::End => Strings::from_values(["end"]),
        }
    }
}

impl From<WhereFrom> for Robj {
    fn from(value: WhereFrom) -> Self {
        Strings::from(value).into()
    }
}

#[extendr]
fn interpolate_point(
    line: Robj,
    value: Robj,
    metric: &str,
    measure: &str,
    from: WhereFrom,
) -> extendr_api::Result<()> {
    let line_chunks = as_linestring_chunks(line)?;
    let value = try_float_array(value, "value")?;

    let metadata = line_chunks[0].data_type().metadata().clone();
    let mut bldr = PointBuilder::new(PointType::new(geoarrow::datatypes::Dimension::XY, metadata));

    macro_rules! dispatch {
        ($metric_val:expr, $method:ident) => {
            for chunk in &line_chunks {
                for (li, vi) in chunk.iter().zip(value.iter()) {
                    if let (Some(Ok(ls)), Some(v)) = (li, vi) {
                        let pt = $metric_val.$method(&ls.to_line_string(), v);
                        bldr.push_point(pt.as_ref());
                    } else {
                        bldr.push_null();
                    }
                }
            }
        };
    }

    match (metric, measure, from) {
        ("euclidean", "ratio", WhereFrom::Start) => dispatch!(Euclidean, point_at_ratio_from_start),
        ("euclidean", "distance", WhereFrom::Start) => {
            dispatch!(Euclidean, point_at_distance_from_start)
        }
        ("haversine", "ratio", WhereFrom::Start) => dispatch!(Haversine, point_at_ratio_from_start),
        ("haversine", "distance", WhereFrom::Start) => {
            dispatch!(Haversine, point_at_distance_from_start)
        }
        ("geodesic", "ratio", WhereFrom::Start) => dispatch!(Geodesic, point_at_ratio_from_start),
        ("geodesic", "distance", WhereFrom::Start) => {
            dispatch!(Geodesic, point_at_distance_from_start)
        }
        ("rhumb", "ratio", WhereFrom::Start) => dispatch!(Rhumb, point_at_ratio_from_start),
        ("rhumb", "distance", WhereFrom::Start) => dispatch!(Rhumb, point_at_distance_from_start),
        ("euclidean", "ratio", WhereFrom::End) => dispatch!(Euclidean, point_at_ratio_from_end),
        ("euclidean", "distance", WhereFrom::End) => {
            dispatch!(Euclidean, point_at_distance_from_end)
        }
        ("haversine", "ratio", WhereFrom::End) => dispatch!(Haversine, point_at_ratio_from_end),
        ("haversine", "distance", WhereFrom::End) => {
            dispatch!(Haversine, point_at_distance_from_end)
        }
        ("geodesic", "ratio", WhereFrom::End) => dispatch!(Geodesic, point_at_ratio_from_end),
        ("geodesic", "distance", WhereFrom::End) => dispatch!(Geodesic, point_at_distance_from_end),
        ("rhumb", "ratio", WhereFrom::End) => dispatch!(Rhumb, point_at_ratio_from_end),
        ("rhumb", "distance", WhereFrom::End) => dispatch!(Rhumb, point_at_distance_from_end),
        (_, "ratio" | "distance", _) => {
            return Err(Error::Other(
                "`metric` must be one of `euclidean`, `haversine`, `geodesic`, or `rhumb`"
                    .to_string(),
            ));
        }
        _ => {
            return Err(Error::Other(
                "`measure` must be one of `distance` or `ratio`".to_string(),
            ));
        }
    }

    Ok(())
}
