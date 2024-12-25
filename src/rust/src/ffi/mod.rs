
use arrow::{
    array::make_array,
    array::RecordBatchReader,
    datatypes::Field,
    ffi::{from_ffi, FFI_ArrowArray, FFI_ArrowSchema},
    ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream},
};
use extendr_api::{Attributes, Error, ExternalPtr, IntoRobj, Robj};

use geoarrow::{
    array::{
        GeometryArray, GeometryCollectionArray, LineStringArray, MultiLineStringArray,
        MultiPointArray, MultiPolygonArray, NativeArrayDyn, PointArray, PolygonArray, RectArray,
    },
    error::GeoArrowError,
    table::Table,
    ArrayBase,
};
use std::borrow::Cow;

pub struct GeoTable(pub Table);

impl From<Table> for GeoTable {
    fn from(value: Table) -> Self {
        GeoTable(value)
    }
}

impl From<GeoTable> for Table {
    fn from(value: GeoTable) -> Self {
        value.0
    }
}

impl TryFrom<Robj> for GeoTable {
    type Error = Error;

    fn try_from(value: Robj) -> Result<Self, Self::Error> {
        let mut inner: ExternalPtr<FFI_ArrowArrayStream> = ExternalPtr::try_from(value)?;

        let s = unsafe { ArrowArrayStreamReader::from_raw(&mut *inner) }
            .map_err(|e| Error::Other(e.to_string()))?;
        let schema = s.schema();

        let mut produced_batches = vec![];
        for batch in s {
            produced_batches.push(batch.map_err(|e| {
                // see if there is a better way to handle this.
                // throw_r_error(e.to_string());
                Error::Other(e.to_string())
            })?);
        }

        let res =
            Table::try_new(produced_batches, schema).map_err(|e| Error::Other(e.to_string()))?;

        Ok(GeoTable(res))
    }
}

impl TryFrom<GeoTable> for Robj {
    type Error = Error;
    fn try_from(value: GeoTable) -> Result<Self, Self::Error> {
        let out = value.0.into_record_batch_reader();
        let mut ptr = ExternalPtr::new(FFI_ArrowArrayStream::new(out)).into_robj();
        ptr.set_class(["geotable", "nanoarrow_array_stream"])?;
        Ok(ptr)
    }
}

/// Helper function to process FFI array and schema to array types
pub fn try_to_native_dyn_array(
    array: FFI_ArrowArray,
    schema: &FFI_ArrowSchema,
) -> Result<NativeArrayDyn, GeoArrowError> {
    let data = unsafe { from_ffi(array, &schema)? };
    let a = make_array(data);
    let field = Field::try_from(schema)?;

    let nda = NativeArrayDyn::from_arrow_array(&a, &field)?;
    Ok(nda)
}

macro_rules! define_try_to_array {
    ($($fn_name:ident, $array_type:ty),*) => {
        $(
            pub fn $fn_name(
                array: FFI_ArrowArray,
                schema: &FFI_ArrowSchema,
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
    try_to_point_array,
    PointArray,
    try_to_linestring_array,
    LineStringArray,
    try_to_polygon_array,
    PolygonArray,
    try_to_multipoint_array,
    MultiPointArray,
    try_to_multilinestring_array,
    MultiLineStringArray,
    try_to_multipolygon_array,
    MultiPolygonArray,
    try_to_geometry_array,
    GeometryArray,
    try_to_geometrycollection_array,
    GeometryCollectionArray,
    try_to_rect_array,
    RectArray
);
