use crate::{
    ffi::{GeoChunks, GeoChunksGeneric},
    HandleError,
};
use extendr_api::prelude::*;
use geoarrow::{algorithm::geo::Centroid, array::PointArray};

#[extendr]
pub fn centroid_(x: GeoChunks) -> Result<GeoChunksGeneric<PointArray>> {
    Ok(GeoChunksGeneric(x.0.centroid().handle_error()?))
}

extendr_module! {
    mod centroid;
    fn centroid_;
}
