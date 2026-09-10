# geoarrowrs
Josiah Parry

Fast and efficient geospatial operations on GeoArrow arrays.

Bindings to the [geoarrow-rs](https://geoarrow.org/geoarrow-rs/),
pure-Rust implementation of [GeoArrow](https://geoarrow.org/) with fast
“geospatial algorithms implemented by the [GeoRust
community](https://georust.org/).”[^1]

Rust 🦀 bindings created using [extendr](https://extendr.rs).

The `{geoarrowrs}` (pronounced “gee-oh-air-oh-arr-ess”) package provides
the following:

- Data I/O readers:
  - Shapefile
  - Flatgeobuf
  - GeoJson
  - Note: use `{geoarrow}` and `{arrow}` R packages for reading parquet.
- Triangulation (Delaunay, Earcut)
- Voronoi cells and edges
- Length
- Distance (pairwise only at the moment)
- Simplification
- Area
- Bounding Boxes
- Extremes
- Rotated rectangles
- Interpolation
- Geometry conversion
- Geometry explosion & flattening
- Affine ops

## Installation

Install the `{geoarrowrs}` from GitHub:

``` r
pak::pak("josiahparry/geoarrowrs")
```

Note that this requires Rust to be installed.

<div class="aside">

Visit [rustup](https://rustup.sh/) to install rust (do it, it’s fun!
believe me!).

</div>

## Usage

Note that {geoarrowrs} is desigend to work with {geoarrow} and
{nanoarrow} R packages, specifically. 
Additionally, special care was taken to work with {arrow} and it's Acero / {dplyr} engines.


[^1]: geoarrow-rs website <https://geoarrow.org/geoarrow-rs/>
