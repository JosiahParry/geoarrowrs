use arrow_extendr::IntoArrowRobj;
use extendr_api::prelude::*;
use geo::{AffineOps, AffineTransform, LineString, MultiLineString, MultiPolygon, Point, Polygon};
use geo_traits::to_geo::{
    ToGeoLineString, ToGeoMultiLineString, ToGeoMultiPolygon, ToGeoPoint, ToGeoPolygon,
};
use geoarrow::array::{
    LineStringBuilder, MultiLineStringBuilder, MultiPolygonBuilder, PointBuilder, PolygonBuilder,
};
use geoarrow::datatypes::{
    Dimension, LineStringType, MultiLineStringType, MultiPolygonType, PointType, PolygonType,
};
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor};

use crate::{
    as_linestring_chunks, as_multilinestring_chunks, as_multipolygon_chunks, as_point_chunks,
    as_polygon_chunks, check_recycle_len, try_float_array,
};

/// The six coefficients for one row, already recycled.
struct Coeffs {
    a: Float64Array,
    b: Float64Array,
    xoff: Float64Array,
    d: Float64Array,
    e: Float64Array,
    yoff: Float64Array,
}

use arrow::array::{Array, Float64Array};

impl Coeffs {
    /// Build the transform for row `i`, or None when any coefficient is missing.
    fn at(&self, i: usize) -> Option<AffineTransform<f64>> {
        let pick = |arr: &Float64Array| {
            let idx = if arr.len() == 1 { 0 } else { i };
            if arr.is_null(idx) {
                None
            } else {
                Some(arr.value(idx))
            }
        };
        Some(AffineTransform::new(
            pick(&self.a)?,
            pick(&self.b)?,
            pick(&self.xoff)?,
            pick(&self.d)?,
            pick(&self.e)?,
            pick(&self.yoff)?,
        ))
    }
}

/// The five geometry types differ only in their builder and push method.
macro_rules! transform_chunks {
    ($chunks:expr, $coeffs:expr, $builder:expr, $to_geo:ident, $push:ident, $null:ty) => {{
        let chunks = $chunks;
        let n: usize = chunks.iter().map(|c| c.len()).sum();
        $coeffs.check(n)?;
        let metadata = chunks[0].data_type().metadata().clone();
        let mut bldr = $builder(metadata);
        let mut i = 0usize;
        for chunk in &chunks {
            for geom in chunk.iter() {
                match (geom, $coeffs.at(i)) {
                    (Some(Ok(g)), Some(t)) => bldr
                        .$push(Some(&g.$to_geo().affine_transform(&t)))
                        .map_err(|e| anyhow::anyhow!("{e}"))?,
                    _ => bldr
                        .$push(None::<&$null>)
                        .map_err(|e| anyhow::anyhow!("{e}"))?,
                }
                i += 1;
            }
        }
        return bldr
            .finish()
            .into_arrow_robj()
            .map_err(|e| anyhow::anyhow!("{e}"));
    }};
}

impl Coeffs {
    /// Every coefficient must be length 1 or the geometry length.
    fn check(&self, n: usize) -> anyhow::Result<()> {
        let pairs = [
            (self.a.len(), "a"),
            (self.b.len(), "b"),
            (self.xoff.len(), "xoff"),
            (self.d.len(), "d"),
            (self.e.len(), "e"),
            (self.yoff.len(), "yoff"),
        ];
        for (len, label) in pairs {
            check_recycle_len(len, n, label).map_err(|e| anyhow::anyhow!("{e}"))?;
        }
        Ok(())
    }
}

/// Apply an arbitrary affine transform
///
/// Transforms every coordinate by the six coefficients of an affine matrix.
/// The geometry type of the output matches the input.
///
/// @details
/// The coefficients map a coordinate to
/// `x' = a * x + b * y + xoff` and `y' = d * x + e * y + yoff`, the same
/// ordering PostGIS `ST_Affine` and Shapely use. The identity transform is
/// `a = 1, b = 0, xoff = 0, d = 0, e = 1, yoff = 0`.
///
/// This is the general form behind [translate()], [scale_xy()], [rotate_around_centroid()],
/// and [skew()]. Reach for those when they fit, since they are clearer at the
/// call site. Use this one to apply a matrix you already have, or to compose
/// several steps into a single pass over the coordinates.
///
/// All six coefficients are recycled against `geometry`, so a different
/// transform can be applied to every row.
///
/// @param geometry a GeoArrow point, linestring, multilinestring, polygon, or
///   multipolygon array
/// @param a,b,d,e the linear part of the matrix; each length 1 or the same
///   length as `geometry`
/// @param xoff,yoff the translation part; each length 1 or the same length as
///   `geometry`
/// @returns a GeoArrow array of the same geometry type as `geometry`
/// @export
/// @family affine
/// @references [AffineOps](https://docs.rs/geo/latest/geo/algorithm/affine_ops/trait.AffineOps.html)
/// @examplesIf requireNamespace("sf", quietly = TRUE) && requireNamespace("geoarrow", quietly = TRUE)
/// square <- sf::st_polygon(list(matrix(
///   c(0, 0, 1, 0, 1, 1, 0, 1, 0, 0), ncol = 2, byrow = TRUE
/// )))
/// g <- geoarrow::as_geoarrow_array(sf::st_sfc(square))
///
/// # double the width and shift right by 10
/// res <- affine_transform(g, a = 2, b = 0, xoff = 10, d = 0, e = 1, yoff = 0)
/// sf::st_as_sfc(geoarrow::as_geoarrow_vctr(res))
#[extendr]
fn affine_transform(
    geometry: Robj,
    a: Robj,
    b: Robj,
    xoff: Robj,
    d: Robj,
    e: Robj,
    yoff: Robj,
) -> anyhow::Result<Robj> {
    let err = |e: extendr_api::Error| anyhow::anyhow!("{e}");
    let coeffs = Coeffs {
        a: try_float_array(a, "a").map_err(err)?,
        b: try_float_array(b, "b").map_err(err)?,
        xoff: try_float_array(xoff, "xoff").map_err(err)?,
        d: try_float_array(d, "d").map_err(err)?,
        e: try_float_array(e, "e").map_err(err)?,
        yoff: try_float_array(yoff, "yoff").map_err(err)?,
    };

    // points are handled by hand because PointBuilder::push_point is infallible
    if let Ok(chunks) = as_point_chunks(geometry.clone()) {
        let n: usize = chunks.iter().map(|c| c.len()).sum();
        coeffs.check(n)?;
        let metadata = chunks[0].data_type().metadata().clone();
        let mut bldr = PointBuilder::new(PointType::new(Dimension::XY, metadata));
        let mut i = 0usize;
        for chunk in &chunks {
            for geom in chunk.iter() {
                match (geom, coeffs.at(i)) {
                    (Some(Ok(g)), Some(t)) => {
                        bldr.push_point(Some(&g.to_point().affine_transform(&t)))
                    }
                    _ => bldr.push_point(None::<&Point<f64>>),
                }
                i += 1;
            }
        }
        return bldr
            .finish()
            .into_arrow_robj()
            .map_err(|e| anyhow::anyhow!("{e}"));
    }

    if let Ok(chunks) = as_linestring_chunks(geometry.clone()) {
        transform_chunks!(
            chunks,
            coeffs,
            |m| LineStringBuilder::new(LineStringType::new(Dimension::XY, m)),
            to_line_string,
            push_line_string,
            LineString<f64>
        );
    }

    if let Ok(chunks) = as_multilinestring_chunks(geometry.clone()) {
        transform_chunks!(
            chunks,
            coeffs,
            |m| MultiLineStringBuilder::new(MultiLineStringType::new(Dimension::XY, m)),
            to_multi_line_string,
            push_multi_line_string,
            MultiLineString<f64>
        );
    }

    if let Ok(chunks) = as_polygon_chunks(geometry.clone()) {
        transform_chunks!(
            chunks,
            coeffs,
            |m| PolygonBuilder::new(PolygonType::new(Dimension::XY, m)),
            to_polygon,
            push_polygon,
            Polygon<f64>
        );
    }

    if let Ok(chunks) = as_multipolygon_chunks(geometry) {
        transform_chunks!(
            chunks,
            coeffs,
            |m| MultiPolygonBuilder::new(MultiPolygonType::new(Dimension::XY, m)),
            to_multi_polygon,
            push_multi_polygon,
            MultiPolygon<f64>
        );
    }

    Err(anyhow::anyhow!(
        "Expected points, (multi)linestrings, or (multi)polygons"
    ))
}

extendr_module! {
    mod matrix;
    fn affine_transform;
}
