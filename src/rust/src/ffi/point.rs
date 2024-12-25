use std::borrow::Cow;

use arrow::{
    array::make_array,
    datatypes::Field,
    ffi::{from_ffi, FFI_ArrowArray, FFI_ArrowSchema},
};
use geoarrow::{
    array::{GeometryArray, GeometryCollectionArray, LineStringArray, MultiLineStringArray, MultiPointArray, MultiPolygonArray, NativeArrayDyn, PointArray, PolygonArray, RectArray},
    error::GeoArrowError,
    ArrayBase, 
};

/// Helper function to process FFI array and schema to array types
pub fn try_to_native_dyn_array(
    array: FFI_ArrowArray,
    schema: FFI_ArrowSchema,
) -> Result<NativeArrayDyn, GeoArrowError> {
    let data = unsafe { from_ffi(array, &schema)? };
    let a = make_array(data);
    let field = Field::try_from(&schema)?;

    let nda = NativeArrayDyn::from_arrow_array(&a, &field)?;
    Ok(nda)
}

macro_rules! define_try_to_array {
    ($($fn_name:ident, $array_type:ty),*) => {
        $(
            pub fn $fn_name(
                array: FFI_ArrowArray,
                schema: FFI_ArrowSchema,
            ) -> Result<$array_type, GeoArrowError> {
                let a = try_to_native_dyn_array(array, schema)?;
                let res = a
                    .as_any()
                    .downcast_ref::<$array_type>()
                    .ok_or(GeoArrowError::IncorrectType(Cow::Owned(
                        concat!("Failed to convert to ", stringify!($array_type)).to_string(),
                    )))?
                    .clone();
                Ok(res)
            }
        )*
    };
}

// Use the macro to define functions for the specified types
define_try_to_array!(
    try_to_point_array, PointArray,
    try_to_linestring_array, LineStringArray,
    try_to_polygon_array, PolygonArray,
    try_to_multipoint_array, MultiPointArray,
    try_to_multilinestring_array, MultiLineStringArray,
    try_to_multipolygon_array, MultiPolygonArray,
    try_to_geometry_array, GeometryArray,
    try_to_geometrycollection_array, GeometryCollectionArray,
    try_to_rect_array, RectArray
);
