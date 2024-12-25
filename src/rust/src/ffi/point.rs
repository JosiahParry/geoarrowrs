use arrow::{
    array::make_array,
    datatypes::Field,
    ffi::{from_ffi, FFI_ArrowArray, FFI_ArrowSchema},
};
use geoarrow::array::NativeArrayDyn;

pub fn try_to_native_dyn_array(
    points: FFI_ArrowArray,
    schema: FFI_ArrowSchema,
) -> Result<NativeArrayDyn, geoarrow::error::GeoArrowError> {
    let data = unsafe { from_ffi(points, &schema)? };
    let a = make_array(data);
    let field = Field::try_from(&schema)?;

    let nda = NativeArrayDyn::from_arrow_array(&a, &field)?;
    Ok(nda)
}
