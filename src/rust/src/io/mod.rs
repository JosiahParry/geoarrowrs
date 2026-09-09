mod flatgeobuf;
mod geojson;
mod shapefile;
use extendr_api::prelude::*;

extendr_module! {
    mod io;
    use flatgeobuf;
    use geojson;
    use shapefile;
}
