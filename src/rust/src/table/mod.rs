use extendr_api::prelude::*;
use geoarrow::algorithm::native::DowncastTable;

use crate::{ffi::GeoTable, HandleError};

#[extendr]
fn downcast_(x: GeoTable) -> Result<GeoTable> {
    x.0.downcast().map(|t| GeoTable(t)).handle_error()
}

extendr_module! {
    mod table;
    fn downcast_;
}
