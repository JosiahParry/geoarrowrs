use std::sync::Arc;

use arrow::array::{ArrayData, Float64Array, Float64Builder};
use arrow_extendr::{FromArrowRobj, geoarrow::GeoArrowVctr};
use extendr_api::prelude::*;
use geoarrow::array::{
    LineStringArray, MultiLineStringArray, MultiPointArray, MultiPolygonArray, PointArray,
    PolygonArray,
};
use geoarrow_array::GeoArrowArray;

pub(crate) mod affine;
pub(crate) mod aggregate;
pub(crate) mod area;
pub(crate) mod bearing;
pub(crate) mod boolean;
pub(crate) mod boundary;
pub(crate) mod cast;
pub(crate) mod cluster;
pub(crate) mod construct;
pub(crate) mod convert;
pub(crate) mod densify;
pub(crate) mod destination;
pub(crate) mod distance;
pub(crate) mod envelope;
pub(crate) mod index;
pub(crate) mod interpolate_line;
pub(crate) mod interpolate_point;
pub(crate) mod intersection;
pub(crate) mod io;
pub(crate) mod iteration;
pub(crate) mod length;
pub(crate) mod misc;
pub(crate) mod query;
pub(crate) mod simplify;
pub(crate) mod sparse;
pub(crate) mod threads;
pub(crate) mod topology;
pub(crate) mod triangulate;
pub(crate) mod validation;
pub(crate) mod voronoi;
pub(crate) mod winding;

fn as_point_chunks(x: Robj) -> extendr_api::Result<Vec<PointArray>> {
    if let Ok(vctr) = GeoArrowVctr::try_from(&x) {
        vctr.as_point_chunks()
            .map_err(|e| Error::Other(e.to_string()))
    } else {
        let arr = PointArray::from_arrow_robj(&x).map_err(|e| Error::Other(e.to_string()))?;
        Ok(vec![arr])
    }
}

fn as_geometry_chunks(x: Robj) -> extendr_api::Result<Vec<Arc<dyn GeoArrowArray>>> {
    if let Ok(vctr) = GeoArrowVctr::try_from(&x) {
        vctr.as_dyn_chunks()
            .map_err(|e| Error::Other(e.to_string()))
    } else {
        let arr = <Arc<dyn GeoArrowArray>>::from_arrow_robj(&x)
            .map_err(|e| Error::Other(e.to_string()))?;
        Ok(vec![arr])
    }
}

fn as_multipoint_chunks(x: Robj) -> extendr_api::Result<Vec<MultiPointArray>> {
    if let Ok(vctr) = GeoArrowVctr::try_from(&x) {
        vctr.as_multipoint_chunks()
            .map_err(|e| Error::Other(e.to_string()))
    } else {
        let arr = MultiPointArray::from_arrow_robj(&x).map_err(|e| Error::Other(e.to_string()))?;
        Ok(vec![arr])
    }
}

fn as_polygon_chunks(x: Robj) -> extendr_api::Result<Vec<PolygonArray>> {
    if let Ok(vctr) = GeoArrowVctr::try_from(&x) {
        vctr.as_polygon_chunks()
            .map_err(|e| Error::Other(e.to_string()))
    } else {
        let arr = PolygonArray::from_arrow_robj(&x).map_err(|e| Error::Other(e.to_string()))?;
        Ok(vec![arr])
    }
}

fn as_multipolygon_chunks(x: Robj) -> extendr_api::Result<Vec<MultiPolygonArray>> {
    if let Ok(vctr) = GeoArrowVctr::try_from(&x) {
        vctr.as_multipolygon_chunks()
            .map_err(|e| Error::Other(e.to_string()))
    } else {
        let arr =
            MultiPolygonArray::from_arrow_robj(&x).map_err(|e| Error::Other(e.to_string()))?;
        Ok(vec![arr])
    }
}

fn as_linestring_chunks(x: Robj) -> extendr_api::Result<Vec<LineStringArray>> {
    if let Ok(vctr) = GeoArrowVctr::try_from(&x) {
        vctr.as_linestring_chunks()
            .map_err(|e| Error::Other(e.to_string()))
    } else {
        let arr = LineStringArray::from_arrow_robj(&x).map_err(|e| Error::Other(e.to_string()))?;
        Ok(vec![arr])
    }
}
fn as_multilinestring_chunks(x: Robj) -> extendr_api::Result<Vec<MultiLineStringArray>> {
    if let Ok(vctr) = GeoArrowVctr::try_from(&x) {
        vctr.as_multilinestring_chunks()
            .map_err(|e| Error::Other(e.to_string()))
    } else {
        let arr =
            MultiLineStringArray::from_arrow_robj(&x).map_err(|e| Error::Other(e.to_string()))?;
        Ok(vec![arr])
    }
}

/// Accept either an arrow float64 array or a plain R numeric vector, so callers can pass `0.1` rather than wrapping it in `nanoarrow::as_nanoarrow_array()`.
pub(crate) fn try_float_array(
    robj: Robj,
    label: &'static str,
) -> extendr_api::Result<Float64Array> {
    if let Ok(arr) = ArrayData::from_arrow_robj(&robj) {
        if arr.data_type().is_floating() {
            return Ok(Float64Array::from(arr));
        }
        return Err(Error::Other(format!(
            "Expected `{label}` to be a float 64 array, got {}",
            arr.data_type()
        )));
    }

    // integer vectors are coerced, so `1L` works as readily as `1`
    let doubles = robj.as_real_vector().or_else(|| {
        robj.as_integer_vector()
            .map(|v| v.into_iter().map(|i| i as f64).collect())
    });

    let Some(values) = doubles else {
        return Err(Error::Other(format!(
            "Expected `{label}` to be a numeric vector or a float 64 array"
        )));
    };

    let mut bldr = Float64Builder::with_capacity(values.len());
    for v in values {
        if v.is_na() {
            bldr.append_null();
        } else {
            bldr.append_value(v);
        }
    }
    Ok(bldr.finish())
}

fn impl_to_geo<'a>(
    array: &'a impl geoarrow_array::GeoArrowArrayAccessor<'a>,
) -> geoarrow::error::GeoArrowResult<Vec<Option<geo::Geometry<f64>>>> {
    use geo_traits::to_geo::ToGeoGeometry;
    let mut out = Vec::with_capacity(array.len());
    for item in array.iter() {
        match item {
            // try_ rather than to_geometry, which panics on an empty point
            Some(g) => out.push(g?.try_to_geometry()),
            None => out.push(None),
        }
    }
    Ok(out)
}

/// Read any geoarrow array as geo geometries. Dispatching on the concrete type means a point or multipolygon array works, not just a mixed one.
pub(crate) fn as_geo_geometries(
    array: &dyn geoarrow_array::GeoArrowArray,
) -> extendr_api::Result<Vec<Option<geo::Geometry<f64>>>> {
    geoarrow_array::downcast_geoarrow_array!(array, impl_to_geo)
        .map_err(|e| Error::Other(e.to_string()))
}

/// Read a point argument as one point per row, recycled against `n`.
pub(crate) fn as_recycled_points(
    robj: Robj,
    n: usize,
    label: &'static str,
) -> extendr_api::Result<Vec<Option<geo::Point<f64>>>> {
    use geo_traits::to_geo::ToGeoPoint;
    use geoarrow_array::GeoArrowArrayAccessor;
    let chunks = as_point_chunks(robj)?;
    let mut out = Vec::new();
    for chunk in &chunks {
        for item in chunk.iter() {
            out.push(match item {
                Some(Ok(p)) => Some(p.to_point()),
                _ => None,
            });
        }
    }
    check_recycle_len(out.len(), n, label)?;
    Ok(out)
}

/// Walk two point arrays in lockstep, recycling `dest`, and apply one metric per row.
///
/// Distance and bearing differ only in the metric, so both go through here.
pub(crate) fn point_metric(
    origin: Robj,
    dest: Robj,
    metric: fn(&geo::Point<f64>, &geo::Point<f64>) -> Option<f64>,
) -> extendr_api::Result<Robj> {
    use arrow_extendr::IntoArrowRobj;
    use geo_traits::to_geo::ToGeoPoint;
    use geoarrow_array::GeoArrowArrayAccessor;

    let origins = as_point_chunks(origin)?;
    let n = origins.iter().map(|c| c.len()).sum();
    let dests = as_recycled_points(dest, n, "dest")?;
    let mut bldr = Float64Builder::with_capacity(n);
    let mut dests = dests.iter().cycle();

    for chunk in &origins {
        for item in chunk.iter() {
            match (item, dests.next().and_then(|d| d.as_ref())) {
                (Some(Ok(o)), Some(d)) => match metric(&o.to_point(), d) {
                    Some(v) => bldr.append_value(v),
                    None => bldr.append_null(),
                },
                _ => bldr.append_null(),
            }
        }
    }

    bldr.finish().into_arrow_robj()
}

/// Walk two geometry arrays in lockstep, recycling `dest`, and apply one metric per row.
pub(crate) fn geometry_metric(
    origin: Robj,
    dest: Robj,
    metric: fn(&geo::Geometry<f64>, &geo::Geometry<f64>) -> f64,
) -> extendr_api::Result<Robj> {
    use arrow_extendr::IntoArrowRobj;

    let origins = as_geometry_chunks(origin)?;
    let n = origins.iter().map(|c| c.len()).sum();
    let dests = as_recycled_geometries(dest, n, "dest")?;
    let mut bldr = Float64Builder::with_capacity(n);
    let mut dests = dests.iter().cycle();

    for chunk in &origins {
        for geom in as_geo_geometries(chunk.as_ref())? {
            match (geom, dests.next().and_then(|d| d.as_ref())) {
                (Some(x), Some(y)) => bldr.append_value(metric(&x, y)),
                _ => bldr.append_null(),
            }
        }
    }

    bldr.finish().into_arrow_robj()
}

/// Read a geometry argument as one geometry per row, recycled against `n`.
pub(crate) fn as_recycled_geometries(
    robj: Robj,
    n: usize,
    label: &'static str,
) -> extendr_api::Result<Vec<Option<geo::Geometry<f64>>>> {
    let chunks = as_geometry_chunks(robj)?;
    let mut out = Vec::new();
    for chunk in &chunks {
        out.extend(as_geo_geometries(chunk.as_ref())?);
    }
    check_recycle_len(out.len(), n, label)?;
    Ok(out)
}

/// Check that two geometry arrays walked in lockstep have the same length, since zipping them would otherwise truncate to the shorter one.
pub(crate) fn check_pair_len(
    a: usize,
    b: usize,
    label_a: &'static str,
    label_b: &'static str,
) -> extendr_api::Result<()> {
    if a != b {
        return Err(Error::Other(format!(
            "`{label_a}` and `{label_b}` must be the same length, got {a} and {b}"
        )));
    }
    Ok(())
}

/// Check that a parameter array is length 1 or the same length as the geometry array, so that it can be recycled with `.cycle()` over `n` geometries.
pub(crate) fn check_recycle_len(
    len: usize,
    n: usize,
    label: &'static str,
) -> extendr_api::Result<()> {
    if len != 1 && len != n {
        return Err(Error::Other(format!(
            "`{label}` must be length 1 or the same length as `geometry` ({n}), got {len}"
        )));
    }
    Ok(())
}
// Macro to generate exports.
// This ensures exported functions are registered with R.
// See corresponding C code in `entrypoint.c`.
extendr_module! {
    mod geoarrowrs;
    use affine;
    use aggregate;
    use area;
    use distance;
    use iteration;
    use length;
    use bearing;
    use destination;
    use query;
    use simplify;
    use sparse;
    use topology;
    use triangulate;
    use misc;
    use boolean;
    use boundary;
    use cast;
    use cluster;
    use construct;
    use convert;
    use densify;
    use envelope;
    use index;
    use intersection;
    use interpolate_line;
    use interpolate_point;
    use io;
    use validation;
    use voronoi;
    use winding;
}
