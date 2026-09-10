# Register geoarrowrs functions with Arrow

Makes geoarrowrs functions callable inside `dplyr` verbs on an Arrow
`Table`, `RecordBatch`, or `Dataset`, so the geometry never has to be
pulled into R.

## Usage

``` r
register_geoarrow_udfs(x, column = "geometry", functions = NULL, prefix = "")
```

## Arguments

- x:

  an Arrow `Table`, `RecordBatch`, or `Dataset` holding a GeoArrow
  column

- column:

  the name of the geometry column to take the type from

- functions:

  names of geoarrowrs functions to register. `NULL`, the default,
  registers everything that can be

- prefix:

  prepended to each registered name, to avoid masking an existing Arrow
  binding

## Value

the registered names, invisibly

## Details

Arrow's query engine only knows the kernels registered with it, so
calling a geoarrowrs function on a `Table` normally warns that the
expression is not supported and falls back to pulling the data into R.
This registers each function as an Arrow scalar kernel instead, which
lets it run inside the engine and, on a `Dataset`, stream batch by batch
without ever holding the whole column in memory.

A kernel is registered for the exact GeoArrow type of `column` in `x`,
since Arrow dispatches on the precise input type. Reading a different
geometry type means calling this again with that data. Output types are
inferred by running each function once against a single row.

Only functions whose arguments are all geometries are registered. Those
taking a numeric argument, such as
[`buffer()`](https://josiahparry.github.io/geoarrowrs/reference/buffer.md)
or
[`simplify()`](https://josiahparry.github.io/geoarrowrs/reference/simplify.md),
are not registered, and neither is
[`unary_union()`](https://josiahparry.github.io/geoarrowrs/reference/unary_union.md),
which is an aggregate rather than a scalar kernel.

## Examples

``` r
library(dplyr)
#> 
#> Attaching package: ‘dplyr’
#> The following object is masked from ‘package:geoarrowrs’:
#> 
#>     contains
#> The following objects are masked from ‘package:stats’:
#> 
#>     filter, lag
#> The following objects are masked from ‘package:base’:
#> 
#>     intersect, setdiff, setequal, union

nc <- as.data.frame(read_shapefile(
  system.file("shape/nc.shp", package = "sf")
))
tbl <- arrow::arrow_table(nc)

register_geoarrow_udfs(tbl)
#> Warning: Could not register 5 functions for this geometry type.
#> ℹ `length_euclidean()`, `length_haversine()`, `length_geodesic()`,
#>   `length_rhumb()`, and `length_vincenty()`
#> ✖ First reason: Invalid argument error: Extension type name mismatch: expected
#>   geoarrow.linestring, got geoarrow.multipolygon

tbl |>
  mutate(area = unsigned_area(geometry)) |>
  select(NAME, area) |>
  head(3) |>
  collect()
#> # A tibble: 3 × 2
#>   NAME        area
#>   <chr>      <dbl>
#> 1 Ashe      0.114 
#> 2 Alleghany 0.0614
#> 3 Surry     0.143 
```
