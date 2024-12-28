use crate::{
    ffi::{GeoChunks, GeoChunksGeneric},
    HandleError,
};
use extendr_api::prelude::*;
use geoarrow::{algorithm::geo::MinimumRotatedRect, array::PolygonArray};

#[extendr]
pub fn minimum_rotated_rect_(x: GeoChunks) -> Result<GeoChunksGeneric<PolygonArray>> {
    let res = x.0.minimum_rotated_rect().handle_error()?;
    Ok(GeoChunksGeneric(res))
}

extendr_module! {
    mod minimum_rotated_rect;
    fn minimum_rotated_rect_;
}
