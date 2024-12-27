pub mod algorithm;
pub mod ffi;
pub mod io;
pub mod length;

use std::sync::Arc;

use arrow::{
    array::{RecordBatch, RecordBatchIterator, RecordBatchReader},
    datatypes::Schema,
    ffi::{FFI_ArrowArray, FFI_ArrowSchema},
    ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream},
};
use extendr_api::prelude::*;
use ffi::{GeoChunks, GeoTable};
use geoarrow::table::Table;

#[extendr]
pub fn read_ffi_array_schema(
    array: ExternalPtr<FFI_ArrowArray>,
    schema: ExternalPtr<FFI_ArrowSchema>,
) -> List {
    list!(array, schema)
}

#[extendr]
fn read_ffi_stream(mut x: ExternalPtr<FFI_ArrowArrayStream>) -> ExternalPtr<FFI_ArrowArrayStream> {
    let res = unsafe { ArrowArrayStreamReader::from_raw(&mut *x) }.unwrap();

    let schema = res.schema();

    let mut produced_batches = vec![];
    for batch in res {
        produced_batches.push(batch);
    }

    let mut out = ExternalPtr::new(FFI_ArrowArrayStream::new(Box::new(
        RecordBatchIterator::new(produced_batches, schema),
    )));

    out.set_class(["nanoarrow_array_stream"]).unwrap();

    out
}

#[extendr]
fn round_trip_geotable(x: GeoTable) -> GeoTable {
    x
}

#[extendr]
fn chunks_from_geoarrow_vctr(x: GeoChunks) -> GeoChunks {
    rprintln!("{x:#?}");
    x
}

#[extendr]
fn read_ffi_geoarrow_tbl(
    mut x: ExternalPtr<FFI_ArrowArrayStream>,
) -> ExternalPtr<FFI_ArrowArrayStream> {
    let s = unsafe { ArrowArrayStreamReader::from_raw(&mut *x) }.unwrap();
    let schema = s.schema();

    let mut produced_batches = vec![];
    for batch in s {
        produced_batches.push(batch.unwrap());
    }
    let res = Table::try_new(produced_batches, schema).unwrap();

    let out = res.into_record_batch_reader();

    ExternalPtr::new(FFI_ArrowArrayStream::new(out))
}

#[extendr]
fn get_geometry_from_table(x: GeoTable) -> ExternalPtr<FFI_ArrowArrayStream> {
    let res = x.0.geometry_column(None).unwrap();
    let ext_field_type = res.as_ref().extension_field();
    let schema = Arc::new(Schema::new(vec![ext_field_type]));
    let mut vecs = vec![];

    // this should be generalized a bit
    for arr in res.array_refs() {
        let rb = RecordBatch::try_new(schema.clone(), vec![arr]);
        vecs.push(rb);
    }

    let rbi = RecordBatchIterator::new(vecs, schema);
    let stream = FFI_ArrowArrayStream::new(Box::new(rbi));
    let mut out = ExternalPtr::new(stream);
    out.set_class(["nanoarrow_array_stream"]).unwrap();
    out
}

// Macro to generate exports.
// This ensures exported functions are registered with R.
// See corresponding C code in `entrypoint.c`.
extendr_module! {
    mod geoarrowrs;
    use length;
    use io;
    fn read_ffi_array_schema;
    fn read_ffi_stream;
    fn read_ffi_geoarrow_tbl;
    fn round_trip_geotable;
    fn get_geometry_from_table;
    fn chunks_from_geoarrow_vctr;
}
