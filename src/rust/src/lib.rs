pub mod ffi; 

use std::io::{BufRead, BufReader};

use extendr_api::prelude::*;
use arrow::{array::{Array, Int32Array, RecordBatch, RecordBatchReader}, ffi::{from_ffi, to_ffi, FFI_ArrowArray, FFI_ArrowSchema}, ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream}};
use geoarrow::table::Table;

#[extendr]
pub fn read_ffi_array_schema(array: ExternalPtr<FFI_ArrowArray>, schema: ExternalPtr<FFI_ArrowSchema>) -> List {
    list!(array, schema)
}

#[extendr]
fn read_ffi_stream(x: ExternalPtr<FFI_ArrowArrayStream>) -> ExternalPtr<FFI_ArrowArrayStream> {
    x
}

#[extendr]
fn read_ffi_geoarrow_tbl(mut x: ExternalPtr<FFI_ArrowArrayStream>) -> ExternalPtr<FFI_ArrowArrayStream> {
    
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
fn read_geojson_(path: &str, batch_size: Option<usize>) -> ExternalPtr<FFI_ArrowArrayStream> {
    let f = std::fs::File::open(path).unwrap();
    let r = BufReader::new(f);
    let res = geoarrow::io::geojson::read_geojson(r, batch_size).unwrap();
    rprintln!("{res:?}");
    let out = res.into_record_batch_reader();

    ExternalPtr::new(FFI_ArrowArrayStream::new(out))
}

// Macro to generate exports.
// This ensures exported functions are registered with R.
// See corresponding C code in `entrypoint.c`.
extendr_module! {
    mod geoarrowrs;
    fn read_ffi_array_schema;
    fn read_ffi_stream;
    fn read_ffi_geoarrow_tbl;
    fn read_geojson_;
}
