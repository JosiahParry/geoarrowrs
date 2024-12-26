use crate::ffi::*;
use arrow::{
    array::Array,  ffi::{to_ffi, FFI_ArrowArray, FFI_ArrowSchema}
};
use extendr_api::prelude::*;
use geoarrow::{algorithm::geo::{EuclideanLength, GeodesicArea, HaversineLength, VincentyLength}, NativeArray};

#[extendr]
pub fn length_euclidean_(
    mut array: ExternalPtr<FFI_ArrowArray>,
    schema: ExternalPtr<FFI_ArrowSchema>,
) -> Result<List> {
    let array = unsafe { FFI_ArrowArray::from_raw(&mut *array) };
    let res = try_to_native_dyn_array(array, schema.as_ref()).unwrap();
    let lens = res.as_ref().euclidean_length().unwrap();
    let (a, s) = to_ffi(&lens.into_data()).unwrap();
    Ok(list!(ExternalPtr::new(a), ExternalPtr::new(s)))
}

#[extendr]
pub fn length_haversine_(
    mut array: ExternalPtr<FFI_ArrowArray>,
    schema: ExternalPtr<FFI_ArrowSchema>,
) -> Result<List> {
    let array = unsafe { FFI_ArrowArray::from_raw(&mut *array) };
    let res = try_to_native_dyn_array(array, schema.as_ref()).unwrap();
    let lens = res.as_ref().haversine_length().unwrap();
    let (a, s) = to_ffi(&lens.into_data()).unwrap();
    Ok(list!(ExternalPtr::new(a), ExternalPtr::new(s)))
}

#[extendr]
pub fn length_geodesic_(
    mut array: ExternalPtr<FFI_ArrowArray>,
    schema: ExternalPtr<FFI_ArrowSchema>,
) -> Result<List> {
    let array = unsafe { FFI_ArrowArray::from_raw(&mut *array) };
    let res = try_to_native_dyn_array(array, schema.as_ref()).unwrap();
    let lens = res.as_ref().geodesic_area_unsigned().unwrap();
    let (a, s) = to_ffi(&lens.into_data()).unwrap();
    Ok(list!(ExternalPtr::new(a), ExternalPtr::new(s)))
}

#[extendr]
pub fn length_vincenty_(
    mut array: ExternalPtr<FFI_ArrowArray>,
    schema: ExternalPtr<FFI_ArrowSchema>,
) -> Result<List> {
    let array = unsafe { FFI_ArrowArray::from_raw(&mut *array) };
    let res = try_to_native_dyn_array(array, schema.as_ref()).unwrap();
    let lens = res.as_ref().vincenty_length().unwrap();
    let (a, s) = to_ffi(&lens.into_data()).unwrap();
    Ok(list!(ExternalPtr::new(a), ExternalPtr::new(s)))
}



extendr_module! {
    mod length;
    fn length_euclidean_;
}
