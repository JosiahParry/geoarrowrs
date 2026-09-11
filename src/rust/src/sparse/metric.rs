use extendr_api::prelude::*;
use geo::{Distance, Euclidean, Geodesic, Geometry, Haversine, Rect, Rhumb};

/// The shortest degree of latitude, so a metre pad converts to a generous one.
const METRES_PER_DEGREE: f64 = 110_574.0;

/// How a sparse search measures the distance between two rows
#[derive(Clone, Copy, PartialEq)]
pub(super) enum Metric {
    Euclidean,
    Haversine,
    Geodesic,
    Rhumb,
}

impl Metric {
    pub(super) fn parse(name: &str) -> extendr_api::Result<Self> {
        match name {
            "euclidean" => Ok(Self::Euclidean),
            "haversine" => Ok(Self::Haversine),
            "geodesic" => Ok(Self::Geodesic),
            "rhumb" => Ok(Self::Rhumb),
            other => Err(Error::Other(format!(
                "`metric` must be one of \"euclidean\", \"haversine\", \"geodesic\", or \"rhumb\", not {other:?}"
            ))),
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Euclidean => "euclidean",
            Self::Haversine => "haversine",
            Self::Geodesic => "geodesic",
            Self::Rhumb => "rhumb",
        }
    }

    /// Whether `geo` defines this metric between points alone.
    fn points_only(self) -> bool {
        self != Self::Euclidean
    }

    /// Refuse a metric that cannot measure the geometries it was handed
    ///
    /// `geo` implements the spherical and ellipsoidal metrics for a pair of
    /// points and nothing else, so there is no distance from a point to the
    /// nearest edge of a polygon to return. Saying so is better than returning
    /// a planar number under a spherical name.
    pub(super) fn check(
        self,
        geoms: &[Option<Geometry<f64>>],
        arg: &str,
    ) -> extendr_api::Result<()> {
        if !self.points_only()
            || geoms
                .iter()
                .flatten()
                .all(|g| matches!(g, Geometry::Point(_)))
        {
            return Ok(());
        }
        Err(Error::Other(format!(
            "`metric = \"{}\"` measures between points, and `{arg}` holds other geometries. \
             Pass `metric = \"euclidean\"`, or reduce `{arg}` to points first.",
            self.name()
        )))
    }

    pub(super) fn distance(self, a: &Geometry<f64>, b: &Geometry<f64>) -> Option<f64> {
        if let Self::Euclidean = self {
            return Some(Euclidean.distance(a, b));
        }
        let (Geometry::Point(a), Geometry::Point(b)) = (a, b) else {
            return None;
        };
        Some(match self {
            Self::Haversine => Haversine.distance(*a, *b),
            Self::Geodesic => Geodesic.distance(*a, *b),
            _ => Rhumb.distance(*a, *b),
        })
    }

    /// How far to grow a box, in coordinate units, to cover `d` in metric units
    ///
    /// A spherical metric measures metres over degree coordinates, so the two
    /// are not the same unit and the box has to be grown by the degrees those
    /// metres could span. A degree of longitude shortens towards the poles, so
    /// the widest latitude the box can reach sets the longitude pad, and past
    /// the pole it is the whole range. Generous on purpose: the box only has to
    /// hold every candidate, and the exact distance decides after.
    pub(super) fn pad(self, d: f64, rect: &Rect<f64>) -> (f64, f64) {
        if let Self::Euclidean = self {
            return (d, d);
        }
        let lat = d / METRES_PER_DEGREE;
        let reach = rect.min().y.abs().max(rect.max().y.abs()) + lat;
        if reach >= 90.0 {
            return (180.0, lat);
        }
        let lon = d / (METRES_PER_DEGREE * reach.to_radians().cos());
        (lon.min(180.0), lat)
    }
}
