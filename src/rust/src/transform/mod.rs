use extendr_api::prelude::*;

use crate::as_geometry_chunks;

#[extendr]
fn ga_transform(x: Robj, crs: &str) -> extendr_api::Result<Robj> {
    as_geometry_chunks(x)?;
    Err(Error::Other(format!(
        "`ga_transform()` is not implemented yet, so it cannot reproject to {crs}"
    )))
}

extendr_module! {
    mod transform;
    fn ga_transform;
}
