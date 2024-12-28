use extendr_api::prelude::*;
use geoarrow::{algorithm::geo::BoundingRect, array::RectArray};

use crate::{
    ffi::{GeoChunks, GeoChunksGeneric},
    HandleError,
};

#[extendr]
pub fn bounding_rect_(x: GeoChunks) -> Result<GeoChunksGeneric<RectArray>> {
    let inner = x.0.bounding_rect().handle_error()?;
    Ok(GeoChunksGeneric(inner))
}
extendr_module! {
    mod bounding_rect;

}
