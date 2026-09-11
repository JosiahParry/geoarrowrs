use std::fs::File;
use std::io::BufReader;

use arrow::record_batch::RecordBatchReader;
use arrow_extendr::IntoArrowRobj;
use extendr_api::prelude::*;
use flatgeobuf::FgbReader;
use geoarrow_flatgeobuf::reader::schema::FlatGeobufSchemaScanner;
use geoarrow_flatgeobuf::reader::{
    FlatGeobufHeaderExt, FlatGeobufReaderOptions, FlatGeobufRecordBatchIterator,
};

/// Read a FlatGeobuf file into a record batch stream
///
/// Reads the geometries together with the feature properties. Unlike the
/// shapefile and GeoJSON readers this one streams, so the whole file is never
/// held in memory at once.
///
/// @details
/// Passing `bbox` uses the file's packed Hilbert R-tree index to skip features
/// that fall outside it, so a spatial subset does not read the whole file.
/// Features come back in the order the file stores them, which for an indexed
/// file is that R-tree order rather than the order they were written in.
///
/// @param path path to a `.fgb` file.
/// @param bbox optionally a length 4 numeric vector of `c(xmin, ymin, xmax, ymax)`
///   used to filter features spatially. `NULL` reads every feature.
/// @returns a `nanoarrow_array_stream` of record batches, holding the property
///   columns followed by a `geometry` column.
/// @examplesIf requireNamespace("sf", quietly = TRUE) && requireNamespace("geoarrow", quietly = TRUE)
/// path <- tempfile(fileext = ".fgb")
/// fp <- system.file("shape/nc.shp", package = "sf")
/// sf::st_write(
///   sf::st_read(fp, quiet = TRUE),
///   path,
///   quiet = TRUE
/// )
///
/// # read into a nanoarrow array stream
/// res <- read_flatgeobuf(path)
/// res
///
/// # convert to a df
/// df <- as.data.frame(res)
/// head(df)
///
/// # only the features intersecting a box
/// head(as.data.frame(read_flatgeobuf(path, c(-79, 35, -78, 36))))
/// @export
/// @family io
#[extendr]
fn read_flatgeobuf(
    path: &str,
    #[extendr(default = "NULL")] bbox: Nullable<Doubles>,
) -> extendr_api::Result<Robj> {
    let bbox = match bbox {
        Nullable::Null => None,
        Nullable::NotNull(b) => {
            if b.len() != 4 {
                return Err(Error::Other(format!(
                    "`bbox` must be length 4 (xmin, ymin, xmax, ymax), got {}",
                    b.len()
                )));
            }
            Some((b.elt(0).0, b.elt(1).0, b.elt(2).0, b.elt(3).0))
        }
    };

    let open = || {
        File::open(path)
            .map(BufReader::new)
            .map_err(|e| Error::Other(format!("failed to open {path}: {e}")))
    };
    let other = |e: geoarrow::error::GeoArrowError| Error::Other(e.to_string());
    let fgb = |e: flatgeobuf::Error| Error::Other(e.to_string());

    let reader = FgbReader::open(open()?).map_err(fgb)?;
    let header = reader.header();

    let properties_schema = match header.properties_schema(true) {
        Some(schema) => schema,
        None => {
            // no column info in the header, so scan features for a schema
            let scan_reader = FgbReader::open(open()?).map_err(fgb)?;
            let mut scanner = FlatGeobufSchemaScanner::new(true);
            scanner
                .process(scan_reader.select_all().map_err(fgb)?, Some(1000))
                .map_err(other)?;
            scanner.finish()
        }
    };

    let geometry_type = header.geoarrow_type(Default::default()).map_err(other)?;

    let selection = match bbox {
        Some((xmin, ymin, xmax, ymax)) => {
            reader.select_bbox(xmin, ymin, xmax, ymax).map_err(fgb)?
        }
        None => reader.select_all().map_err(fgb)?,
    };

    let options = FlatGeobufReaderOptions::new(properties_schema, geometry_type);
    let iter = FlatGeobufRecordBatchIterator::try_new(selection, options).map_err(other)?;

    let reader: Box<dyn RecordBatchReader + Send> = Box::new(iter);
    reader.into_arrow_robj()
}

extendr_module! {
    mod flatgeobuf;
    fn read_flatgeobuf;
}
