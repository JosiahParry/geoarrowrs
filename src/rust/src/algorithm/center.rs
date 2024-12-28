use extendr_api::prelude::*;
use geoarrow::{algorithm::geo::Center, array::PointArray};

use crate::{
    ffi::{GeoChunks, GeoChunksGeneric},
    HandleError,
};

#[extendr]
pub fn center_(x: GeoChunks) -> Result<GeoChunksGeneric<PointArray>> {
    Ok(GeoChunksGeneric(x.0.center().handle_error()?))
}
extendr_module! {
    mod center;
    fn center_;
}
