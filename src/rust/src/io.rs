use std::io::BufReader;
use extendr_api::prelude::*;

use crate::ffi::GeoTable;

#[extendr]
fn read_geojson_(path: &str, batch_size: Option<usize>) -> GeoTable {
  let f = std::fs::File::open(path).unwrap();
  let r = BufReader::new(f);
  let res = geoarrow::io::geojson::read_geojson(r, batch_size).unwrap();
  GeoTable(res)
}

#[extendr]
fn read_geojson_lines_(path: &str, batch_size: Option<usize>) -> GeoTable {
  let f = std::fs::File::open(path).unwrap();
  let r = BufReader::new(f);
  let res = geoarrow::io::geojson_lines::read_geojson_lines(r, batch_size).unwrap();
  GeoTable(res)
}

#[extendr]
fn read_flatgeobuf_(path: &str) -> GeoTable {
  let f = std::fs::File::open(path).unwrap();
  let mut r = BufReader::new(f);
  let res = geoarrow::io::flatgeobuf::read_flatgeobuf(&mut r, geoarrow::io::flatgeobuf::FlatGeobufReaderOptions::default()).unwrap();
  GeoTable(res)
}



extendr_module! {
  mod io;
  fn read_geojson_;
  fn read_geojson_lines_;
  fn read_flatgeobuf_;
}