use arrow::{
    array::{make_array, ArrowPrimitiveType, OffsetSizeTrait, RecordBatchReader},
    datatypes::Field,
    ffi::{from_ffi, to_ffi, FFI_ArrowArray, FFI_ArrowSchema},
    ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream},
};
use extendr_api::prelude::*;
use geoarrow::{
    array::{
        GeometryArray, GeometryCollectionArray, LineStringArray, MultiLineStringArray,
        MultiPointArray, MultiPolygonArray, NativeArrayDyn, PointArray, PolygonArray, RectArray,
        SerializedArrayDyn,
    },
    chunked_array::ChunkedGeometryArray,
    error::GeoArrowError,
    table::Table,
    ArrayBase, NativeArray,
};
use std::borrow::Cow;

#[derive(Debug, Clone)]
pub struct GeoChunks(pub ChunkedGeometryArray<NativeArrayDyn>);

impl TryFrom<Robj> for GeoChunks {
    type Error = Error;

    fn try_from(value: Robj) -> Result<Self> {
        let chunks_list = List::try_from(
            value
                .get_attrib("chunks")
                .ok_or(Error::Other(String::from("Expected `chunks` attribute")))?,
        )?;

        let schema = value
            .get_attrib("schema")
            .ok_or(Error::Other(String::from("Expected `schema` attribute")))?;

        // Fetch the FFI Schema
        let ffi_schema: ExternalPtr<FFI_ArrowSchema> = schema.try_into()?;

        let array_chunks: Vec<NativeArrayDyn> = chunks_list
            .into_iter()
            .map(|(_, ci)| {
                let mut ptr = ExternalPtr::<FFI_ArrowArray>::try_from(ci).map_err(|_| {
                    Error::Other(String::from(
                        "Failed to convert chunk to ExternalPtr<FFI_ArrowArray>",
                    ))
                })?;

                let res = unsafe { FFI_ArrowArray::from_raw(ptr.addr_mut()) };

                try_to_native_dyn_array(res, &ffi_schema).map_err(|e| Error::Other(e.to_string()))
            })
            .collect::<Result<Vec<_>>>()?; // Collect results or propagate error

        let res = ChunkedGeometryArray::new(array_chunks);

        Ok(Self(res))
    }
}

impl From<GeoChunks> for Robj {
    fn from(value: GeoChunks) -> Self {
        let inner = value.0;

        let n = inner.len();
        let mut container = Integers::from_iter((1..=n).into_iter().map(|i| Rint::from(i as i32)));

        let offsets_raw = inner.map(|i| i.len());

        let mut offsets = vec![0];
        offsets.extend(offsets_raw);

        let offsets = offsets
            .into_iter()
            .map(|i| Rint::from(i as i32))
            .collect::<Integers>();

        eprintln!("\noffsets calculated");
        let field = inner.extension_field();
        eprintln!("\nfound extension field ");

        let mut ffi_schema = ExternalPtr::new(
            FFI_ArrowSchema::try_from(&field)
                .expect("Failed to create `FFI_ArrowSchema` from `GeoChunks`"),
        );

        ffi_schema
            .set_class(["nanoarrow_schema"])
            .expect("Failed to set nanoarrow_schema class");

        eprintln!("\nffi_schema created");

        let chunk_ptrs = inner
            .chunks()
            .into_iter()
            .map(|chunk| {
                eprintln!("\n casting as array ref");
                let schema = FFI_ArrowSchema::try_from(&chunk.extension_field()).unwrap();
                let chunk = chunk.clone().to_array_ref();

                let it = chunk.to_data();
                // let ffi = FFI_ArrowArray::new(&it);
                let (array, _) = to_ffi(&it).expect("Failed to cast array to FFI_ArrowArray");
                rprintln!("{schema:?}");

                let mut ptr = ExternalPtr::new(array);
                ptr.set_class(["nanoarrow_array"])
                    .expect("Failed to set nanoarrow_array class");

                let mut schema_ptr = ExternalPtr::new(schema);
                schema_ptr.set_class(["nanoarrow_schema"]).unwrap();

                // set the pointer
                unsafe { libR_sys::R_SetExternalPtrTag(ptr.get(), schema_ptr.get()) };
                ptr
            })
            .collect::<List>();

        eprintln!("\ncollected chunks");

        container
            .set_attrib("chunks", chunk_ptrs)
            .expect("Failed to set `chunks` attribute");

        container
            .set_attrib("schema", ffi_schema)
            .expect("Failed to set FFI_ArrowSchema to Robj");

        container
            .set_class(["geoarrow_vctr", "nanoarrow_vctr"])
            .expect("faield to set geoarrow_vctr class");

        container
            .set_attrib("offsets", offsets)
            .expect("Failed to set offsets");

        container.into()
    }
}
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

    fn try_from(value: Robj) -> std::result::Result<Self, Self::Error> {
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

impl From<GeoTable> for Robj {
    fn from(value: GeoTable) -> Self {
        let out = value.0.into_record_batch_reader();
        let mut ptr = ExternalPtr::new(FFI_ArrowArrayStream::new(out)).into_robj();
        ptr.set_class(["geotable", "nanoarrow_array_stream"])
            .expect("failed to set class");
        ptr
    }
}

/// Helper function to process FFI array and schema to array types
pub fn try_to_native_dyn_array(
    array: FFI_ArrowArray,
    schema: &FFI_ArrowSchema,
) -> std::result::Result<NativeArrayDyn, GeoArrowError> {
    let data = unsafe { from_ffi(array, &schema)? };
    let a = make_array(data);
    let field = Field::try_from(schema)?;

    // let nda = NativeArrayDyn::from_arrow_array(&a, &field)?;
    let nda = NativeArrayDyn::from_arrow_array(&a, &field);
    let nda = match nda {
        Ok(r) => r,
        Err(e) => {
            let _serialized_array =
                SerializedArrayDyn::from_arrow_array(&a, &field).map_err(|ee| {
                    let e_msg = format!("Errors:\n  - {e}\n  - {ee}");
                    GeoArrowError::General(e_msg)
                })?;

            return Err(GeoArrowError::NotYetImplemented(
                "Found a WKB/WKT array. This is not yet supported in geoarrow-rs".to_string(),
            ));
            // let res = GeometryArray::try_from(serialized_array.into_array_ref().as_ref())?;
            // NativeArrayDyn::new(Arc::new(res))
        }
    };

    Ok(nda)
}

macro_rules! define_try_to_array {
    ($($fn_name:ident, $array_type:ty),*) => {
        $(
            pub fn $fn_name(
                array: FFI_ArrowArray,
                schema: &FFI_ArrowSchema,
            ) -> std::result::Result<$array_type, GeoArrowError> {
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
