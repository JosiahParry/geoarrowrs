use arrow::{array::Array, ffi::{to_ffi, FFI_ArrowArray, FFI_ArrowSchema}};
use extendr_api::prelude::*;
use geoarrow::{algorithm::geo::Area, NativeArray};

use crate::ffi::{try_to_native_dyn_array, GeoTable};

#[extendr]
pub fn area_euclidean_unsigned_(
    mut array: ExternalPtr<FFI_ArrowArray>,
    schema: ExternalPtr<FFI_ArrowSchema>,
) -> Result<List> {
    let array = unsafe { FFI_ArrowArray::from_raw(&mut *array) };
    let res = try_to_native_dyn_array(array, schema.as_ref()).unwrap();

    let area = res.as_ref().unsigned_area().unwrap();
    let (a, s) = to_ffi(&area.into_data()).unwrap();
    Ok(list!(ExternalPtr::new(a), ExternalPtr::new(s)))
}


extendr_module! {
    mod area;
}