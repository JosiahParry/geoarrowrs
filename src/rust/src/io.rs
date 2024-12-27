use extendr_api::prelude::*;
use geoarrow::{
    io::{
        flatgeobuf::{self, FlatGeobufReaderOptions},
        parquet::{
            write_geoparquet, GeoParquetReaderOptions, GeoParquetRecordBatchReaderBuilder,
            GeoParquetWriterOptions,
        },
        shapefile::ShapefileReaderOptions,
    },
    scalar::{Coord, Rect},
};
use parquet::{basic::Compression, file::properties::WriterProperties};
use std::io::BufReader;

use crate::ffi::GeoTable;

#[extendr]
fn read_geojson_(path: &str, batch_size: Option<usize>) -> Result<GeoTable> {
    let f = std::fs::File::open(path).unwrap();
    let r = BufReader::new(f);
    let res = geoarrow::io::geojson::read_geojson(r, batch_size).unwrap();
    Ok(GeoTable(res))
}

#[extendr]
fn read_geojson_lines_(path: &str, batch_size: Option<usize>) -> GeoTable {
    let f = std::fs::File::open(path).unwrap();
    let r = BufReader::new(f);
    let res = geoarrow::io::geojson_lines::read_geojson_lines(r, batch_size).unwrap();
    GeoTable(res)
}

fn process_bbox(bbox: Doubles) -> Option<(f64, f64, f64, f64)> {
    if bbox.len() < 4 {
        return None;
    }

    let mut res_bbox_iter = bbox.into_iter().take(4).map(|i| i.inner());

    Some((
        res_bbox_iter.next().unwrap(),
        res_bbox_iter.next().unwrap(),
        res_bbox_iter.next().unwrap(),
        res_bbox_iter.next().unwrap(),
    ))
}

fn process_fgb_opts(bbox: Doubles) -> FlatGeobufReaderOptions {
    let mut opts = FlatGeobufReaderOptions::default();
    let bbox = process_bbox(bbox);
    opts.bbox = bbox;
    opts
}

#[extendr]
/// @export
fn read_flatgeobuf_(path: &str, bbox: Doubles) -> Result<GeoTable> {
    use geoarrow::io::flatgeobuf::read_flatgeobuf;

    // FIXME: do additional validation outside of this function
    let opts = process_fgb_opts(bbox);
    let f = std::fs::File::open(path).map_err(|e| Error::Other(e.to_string()))?;
    let mut r = BufReader::new(f);
    let res = read_flatgeobuf(&mut r, opts).map_err(|e| Error::Other(e.to_string()))?;
    Ok(GeoTable(res))
}

#[extendr]
fn read_shapefile_(path: &str) -> Result<GeoTable> {
    use geoarrow::io::shapefile::{read_shapefile, ShapefileReaderOptions};
    use std::io::{BufReader, Read};

    let og_path = std::path::Path::new(path);

    // Check if the path has a valid extension
    if og_path.extension().and_then(|ext| ext.to_str()) != Some("shp") {
        return Err(Error::Other(String::from(
            "Input file must have a .shp extension",
        )));
    }

    // Open the .shp file
    let shp_file = std::fs::File::open(path)
        .map_err(|_| Error::Other(String::from("Unable to open .shp file")))?;
    let shp_reader = BufReader::new(shp_file);

    // Create the path for the .dbf file
    let dbf_path = og_path.with_extension("dbf");

    // Open the .dbf file
    let dbf_file = std::fs::File::open(&dbf_path)
        .map_err(|_| Error::Other(String::from("Unable to open corresponding .dbf file")))?;
    let dbf_reader = BufReader::new(dbf_file);

    // Read the shapefile
    let mut opts = ShapefileReaderOptions::default();

    // FIXME CRS isn't being set by geoarrow-r
    // Attempt to read the .prj file (optional)
    let prj_path = og_path.with_extension("prj");
    let prj_content = if prj_path.exists() {
        let mut prj_file = std::fs::File::open(&prj_path)
            .map_err(|_| Error::Other(String::from("Unable to open .prj file")))?;
        let mut prj_string = String::new();
        prj_file
            .read_to_string(&mut prj_string)
            .map_err(|_| Error::Other(String::from("Failed to read .prj file content")))?;
        Some(prj_string)
    } else {
        None
    };

    opts.crs = prj_content;

    Ok(GeoTable(
        read_shapefile(shp_reader, dbf_reader, opts).map_err(|e| Error::Other(e.to_string()))?,
    ))
}

fn process_row_groups(row_groups: Robj) -> Option<Vec<usize>> {
    let row_groups = row_groups.as_integers()?;
    row_groups
        .into_iter()
        .map(|i| {
            if i < &Rint::from(0) {
                return None;
            };

            if i.is_na() {
                return None;
            };

            Some(i.inner() as usize)
        })
        .collect::<Option<Vec<usize>>>()
}

// GeoParquet
fn geoparquet_reader_options_(
    batch_size: Option<usize>,
    bbox: Robj,
    limit: Option<usize>,
    offset: Option<usize>,
    row_groups: Robj,
) -> GeoParquetReaderOptions {
    let mut opts = GeoParquetReaderOptions::default();

    if let Some(size) = batch_size {
        opts = opts.with_batch_size(size);
    }

    if let Some(lim) = limit {
        opts = opts.with_limit(lim);
    }

    if let Some(off) = offset {
        opts = opts.with_offset(off);
    }

    if let Ok(bbox) = Doubles::try_from(bbox) {
        if let Some(bb) = process_bbox(bbox) {
            let c1 = geo_types::coord! {
                x: bb.0, y: bb.1
            };
            let c2 = geo_types::coord! {
                x: bb.2, y: bb.3
            };
            let r = geo_types::Rect::new(c1, c2);

            // FIXME implement bbox_paths?
            opts = opts.with_bbox(r, None);
        }
    }

    if let Some(row_groups) = process_row_groups(row_groups) {
        opts = opts.with_row_groups(row_groups);
    }

    opts
}

#[extendr]
fn read_geoparquet_(
    path: &str,
    batch_size: Option<usize>,
    bbox: Robj,
    limit: Option<usize>,
    offset: Option<usize>,
    row_groups: Robj,
) -> Result<GeoTable> {
    let opts = geoparquet_reader_options_(batch_size, bbox, limit, offset, row_groups);
    let f = std::fs::File::open(path).map_err(|e| Error::Other(e.to_string()))?;
    let reader =
        GeoParquetRecordBatchReaderBuilder::try_new_with_options(f, Default::default(), opts)
            .map_err(|e| Error::Other(e.to_string()))?
            .build()
            .map_err(|e| Error::Other(e.to_string()))?;
    let table = reader
        .read_table()
        .map_err(|e| Error::Other(e.to_string()))?;

    Ok(GeoTable(table))
}

#[extendr]
fn write_geoparquet_(x: GeoTable, path: &str) -> Result<()> {
    use std::io::BufWriter;

    // TODO enable customizing writer properties
    let parquet_props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();

    let f = std::fs::File::create_new(path).map_err(|e| Error::Other(e.to_string()))?;

    let batches = x.0.into_record_batch_reader();
    let writer = BufWriter::new(f);
    let mut opts = GeoParquetWriterOptions::default();
    opts.writer_properties = Some(parquet_props);

    write_geoparquet(batches, writer, &opts).map_err(|e| Error::Other(e.to_string()))?;

    Ok(())
}

extendr_module! {
  mod io;
  fn read_geojson_;
  fn read_geojson_lines_;
  fn read_flatgeobuf_;
  fn read_shapefile_;
  fn read_geoparquet_;
  fn write_geoparquet_;
}
