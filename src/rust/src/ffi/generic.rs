use arrow::ffi::{to_ffi, FFI_ArrowSchema};
use extendr_api::prelude::*;
use geoarrow::{chunked_array::ChunkedGeometryArray, ArrayBase};

#[derive(Debug, Clone)]
pub struct GeoChunksGeneric<G: ArrayBase>(pub ChunkedGeometryArray<G>);

impl<G: ArrayBase> From<GeoChunksGeneric<G>> for Robj {
    fn from(value: GeoChunksGeneric<G>) -> Self {
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

        let field = inner.extension_field();

        let mut ffi_schema = ExternalPtr::new(
            FFI_ArrowSchema::try_from(&field)
                .expect("Failed to create `FFI_ArrowSchema` from `GeoChunks`"),
        );

        ffi_schema
            .set_class(["nanoarrow_schema"])
            .expect("Failed to set nanoarrow_schema class");

        let chunk_ptrs = inner
            .chunks()
            .into_iter()
            .map(|chunk| {
                let schema = FFI_ArrowSchema::try_from(&chunk.extension_field()).unwrap();
                let chunk = chunk.to_array_ref();

                let it = chunk.to_data();
                // let ffi = FFI_ArrowArray::new(&it);
                let (array, _) = to_ffi(&it).expect("Failed to cast array to FFI_ArrowArray");

                let mut ptr = ExternalPtr::new(array);
                ptr.set_class(["nanoarrow_array"])
                    .expect("Failed to set nanoarrow_array class");

                let mut schema_ptr = ExternalPtr::new(schema);
                schema_ptr
                    .set_class(["nanoarrow_schema"])
                    .expect("Failed to set nanoarrow_schema class");

                // set the pointer
                unsafe { libR_sys::R_SetExternalPtrTag(ptr.get(), schema_ptr.get()) };
                ptr
            })
            .collect::<List>();

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
