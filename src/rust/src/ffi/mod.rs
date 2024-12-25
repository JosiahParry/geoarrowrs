use arrow::{
    array::RecordBatchReader,
    ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream},
};
use extendr_api::{throw_r_error, Attributes, Error, ExternalPtr, IntoRobj, Robj};
use geoarrow::table::Table;

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
                throw_r_error(e.to_string());
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
