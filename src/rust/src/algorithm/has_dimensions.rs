use crate::{
    ffi::{BooleanChunks, GeoChunks},
    HandleError,
};
use extendr_api::prelude::*;
use geoarrow::algorithm::geo::HasDimensions;

#[extendr]
pub fn has_dimensions_(x: GeoChunks) -> Result<BooleanChunks> {
    Ok(BooleanChunks(HasDimensions::is_empty(&x.0).handle_error()?))
}

extendr_module! {
    mod has_dimensions;
    fn has_dimensions_;
}
