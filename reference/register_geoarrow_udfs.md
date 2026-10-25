# Register geoarrowrs functions with Arrow

Makes geoarrowrs functions callable inside `dplyr` verbs on an Arrow
`Table`, `RecordBatch`, or `Dataset`, so you never have to pull the
geometry into R.

## Usage

``` r
register_geoarrow_udfs(crs = NULL, functions = NULL, prefix = "")
```

## Arguments

- crs:

  the coordinate reference system the kernels dispatch on. Pass `NULL`,
  the default, for data with no CRS, a CRS string, or any object
  carrying one: an Arrow `Table`, `RecordBatch`, `Dataset`, `Schema`,
  `Array`, or a GeoArrow array or vector. Pass a list to cover several
  at once

- functions:

  names of geoarrowrs functions to register, or `NULL`, the default, for
  every function `register_geoarrow_udfs()` supports

- prefix:

  a string to prepend to each registered name, to avoid masking an
  existing Arrow binding

## Value

the registered names, invisibly

## Details

Arrow's query engine only knows the kernels you register with it, so
when you call a geoarrowrs function on a `Table`, Arrow warns that it
does not support the expression and pulls the data into R instead.
`register_geoarrow_udfs()` registers each function as an Arrow scalar
kernel, so the engine runs it and, on a `Dataset`, streams it batch by
batch without ever holding the whole column in memory.

`register_geoarrow_udfs()` registers per CRS, not per table. It
registers a kernel for every GeoArrow geometry type, so one call covers
points, linestrings, polygons, and their multi variants at once. It
registers no kernel for a type a function cannot handle, such as
[`ga_length_euclidean()`](https://josiahparry.github.io/geoarrowrs/reference/length.md)
on polygons.

It registers the arguments that are not geometry too. Numeric arguments
such as the `epsilon` of
[`ga_simplify()`](https://josiahparry.github.io/geoarrowrs/reference/ga_simplify.md)
take a `double` column or a literal, which the kernel recycles row by
row. Option arguments such as the `metric` of
[`ga_densify()`](https://josiahparry.github.io/geoarrowrs/reference/ga_densify.md)
take a literal string. Arrow has no optional arguments, so you must pass
every argument of a registered function.

It does not register
[`ga_unary_union()`](https://josiahparry.github.io/geoarrowrs/reference/ga_unary_union.md),
[`ga_explode()`](https://josiahparry.github.io/geoarrowrs/reference/ga_explode.md),
or
[`ga_flatten()`](https://josiahparry.github.io/geoarrowrs/reference/ga_flatten.md),
because they change the length of the array and a scalar kernel may not.

It also registers kernels for a bare `binary` column of WKB, which is
what plain Parquet writes. The kernel parses the WKB and returns a
geometry result as WKB, since a kernel's output type cannot depend on
what the column holds. To work on native GeoArrow instead, cast once
with
[`ga_as_point()`](https://josiahparry.github.io/geoarrowrs/reference/ga_as_point.md)
or one of its siblings and follow it with
[`compute()`](https://dplyr.tidyverse.org/reference/compute.html).
Without the
[`compute()`](https://dplyr.tidyverse.org/reference/compute.html), Arrow
inlines the cast into each expression that reads it and runs it once per
use rather than once per column.

A GeoArrow type carries its CRS, and Arrow compares it when it looks for
a kernel, so you have to pass a `crs` that matches the data. Pass every
CRS you need in one call: when you register a name again,
`register_geoarrow_udfs()` replaces the kernels it had.

geoarrowrs calls `register_geoarrow_udfs()` once with `crs = NULL` when
it loads, so you can use data with no CRS without any setup. For data
with a CRS, you still need to call it with that CRS. Set
`options(geoarrowrs.register_udfs = FALSE)` before loading to skip the
call, which also keeps geoarrowrs from loading arrow when it loads.

## Examples

``` r
library(dplyr)
#> 
#> Attaching package: ‘dplyr’
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

register_geoarrow_udfs(crs = tbl)

tbl |>
  mutate(area = ga_unsigned_area(geometry)) |>
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
