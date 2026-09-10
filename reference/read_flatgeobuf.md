# Read a FlatGeobuf file into a record batch stream

Reads the geometries together with the feature properties. Unlike the
shapefile and GeoJSON readers this one streams, so the whole file is
never held in memory at once.

## Usage

``` r
read_flatgeobuf(path, bbox = NULL)
```

## Arguments

- path:

  path to a `.fgb` file.

- bbox:

  optionally a length 4 numeric vector of `c(xmin, ymin, xmax, ymax)`
  used to filter features spatially. `NULL` reads every feature.

## Value

a `nanoarrow_array_stream` of record batches, holding the property
columns followed by a `geometry` column.

## Details

The geometry type and the property schema are taken from the file
header. If the header carries no column information, up to 1000 features
are scanned to infer one.

Passing `bbox` uses the file's packed Hilbert R-tree index to skip
features that fall outside it, so a spatial subset does not read the
whole file.

Features come back in the order the file stores them, which for an
indexed file is Hilbert R-tree order rather than the order they were
written in.

## See also

Other io:
[`read_geojson()`](https://josiahparry.github.io/geoarrowrs/reference/read_geojson.md),
[`read_shapefile()`](https://josiahparry.github.io/geoarrowrs/reference/read_shapefile.md)

## Examples

``` r
path <- tempfile(fileext = ".fgb")
sf::st_write(sf::st_read(system.file("shape/nc.shp", package = "sf"), quiet = TRUE),
             path, quiet = TRUE)

df <- as.data.frame(read_flatgeobuf(path))
dim(df)
#> [1] 100  15

# only the features intersecting the box
nrow(as.data.frame(read_flatgeobuf(path, c(-79, 35, -78, 36))))
#> [1] 15
```
