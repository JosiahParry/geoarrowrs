use crate::{
    ffi::{GeoChunks, GeoChunksGeneric},
    HandleError,
};
use extendr_api::prelude::*;
use geoarrow::{algorithm::geo::ConvexHull, array::PolygonArray};

#[extendr]
pub fn convex_hull_(x: GeoChunks) -> Result<GeoChunksGeneric<PolygonArray>> {
    let res = x.0.convex_hull().handle_error()?;
    Ok(GeoChunksGeneric(res))
}

extendr_module! {
    mod convex_hull;
    fn convex_hull_;
}
