# geoarrowrs

Josiah Parry

Fast and efficient geospatial operations on GeoArrow arrays.

Bindings to the [geoarrow-rs](https://geoarrow.org/geoarrow-rs/),
pure-Rust implementation of [GeoArrow](https://geoarrow.org/) with fast
“geospatial algorithms implemented by the [GeoRust
community](https://georust.org/).”[^1]

[extendr](https://extendr.rs) creates the Rust 🦀 bindings.

The [geoarrowrs](https://josiahparry.github.io/geoarrowrs/) (pronounced
“gee-oh-air-oh-arr-ess”) package provides the following:

- Data I/O readers:
  - Shapefile
  - Flatgeobuf
  - GeoJson
  - Note: use [geoarrow](https://geoarrow.org/geoarrow-r/) and
    [arrow](https://github.com/apache/arrow/) R packages for reading
    parquet.
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

Install the [geoarrowrs](https://josiahparry.github.io/geoarrowrs/) from
GitHub:

``` r

pak::pak("josiahparry/geoarrowrs")
```

Note that you need Rust to install it.

Visit [rustup](https://rustup.sh/) to install rust (do it, it’s fun!
believe me!).

## Usage

Note that {geoarrowrs} works with the {geoarrow} and {nanoarrow} R
packages, specifically. Additionally, I took special care to make it
work with {arrow} and its Acero / {dplyr} engines.

[^1]: geoarrow-rs website <https://geoarrow.org/geoarrow-rs/>
