use crate::ffi::*;
use arrow::{
    array::{Array, ArrayData},
    ffi::{to_ffi, FFI_ArrowArray, FFI_ArrowSchema}, ffi_stream::FFI_ArrowArrayStream,
};
use extendr_api::prelude::*;
use geoarrow::{algorithm::geo::EuclideanLength, NativeArray};

#[extendr]
pub fn length_euclidean_(
    mut array: ExternalPtr<FFI_ArrowArray>,
    schema: ExternalPtr<FFI_ArrowSchema>,
) -> Result<List> {
    // We leak the pointer so that the arrow array still exists even after
    let ba = Box::new(array.clone());
    let leaked = Box::leak(ba);
    let array = unsafe { FFI_ArrowArray::from_raw(&mut *array) };
    let res = try_to_native_dyn_array(array, schema.as_ref()).unwrap();
    let lens = res.as_ref().euclidean_length().unwrap();
    let (a, s) = to_ffi(&lens.into_data()).unwrap();
    Ok(list!(ExternalPtr::new(a), ExternalPtr::new(s)))
}

extendr_module! {
    mod length;
    fn length_euclidean_;
}
