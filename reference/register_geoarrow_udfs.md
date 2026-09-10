# Register geoarrowrs functions with Arrow

Makes geoarrowrs functions callable inside `dplyr` verbs on an Arrow
`Table`, `RecordBatch`, or `Dataset`, so the geometry never has to be
pulled into R.

## Usage

``` r
register_geoarrow_udfs(crs = NULL, functions = NULL, prefix = "")
```

## Arguments

- crs:

  the coordinate reference system the kernels dispatch on. `NULL`, the
  default, registers for data with no CRS. Also accepts a CRS string, or
  any object carrying one: an Arrow `Table`, `RecordBatch`, `Dataset`,
  `Schema`, `Array`, or a GeoArrow array or vector. Give a list to cover
  several at once

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

Registration is per CRS, not per table. Every GeoArrow geometry type
gets its own kernel, so one call covers points, linestrings, polygons,
and their multi variants at once. A function that cannot handle a type,
such as
[`ga_length_euclidean()`](https://josiahparry.github.io/geoarrowrs/reference/length.md)
against polygons, simply has no kernel for it.

Arguments that are not geometry are registered too. Numeric arguments
such as the `epsilon` of
[`ga_simplify()`](https://josiahparry.github.io/geoarrowrs/reference/ga_simplify.md)
take a `double` column or a literal and are recycled row by row. Option
arguments such as the `metric` of
[`ga_densify()`](https://josiahparry.github.io/geoarrowrs/reference/ga_densify.md)
take a literal string. Since Arrow has no optional arguments, every
argument of a registered function must be given.

[`ga_unary_union()`](https://josiahparry.github.io/geoarrowrs/reference/ga_unary_union.md),
[`ga_explode()`](https://josiahparry.github.io/geoarrowrs/reference/ga_explode.md),
and
[`ga_flatten()`](https://josiahparry.github.io/geoarrowrs/reference/ga_flatten.md)
are not registered, because they change the length of the array and a
scalar kernel may not.

A bare `binary` column of WKB, which is what plain Parquet writes, is
registered for too. The kernel parses it, and a geometry result comes
back as WKB, since a kernel's output type cannot depend on what the
column turns out to hold. To work on native GeoArrow instead, cast once
with
[`ga_as_point()`](https://josiahparry.github.io/geoarrowrs/reference/ga_as_point.md)
or one of its siblings and follow it with
[`compute()`](https://dplyr.tidyverse.org/reference/compute.html).
Without the
[`compute()`](https://dplyr.tidyverse.org/reference/compute.html) Arrow
inlines the cast into each expression that reads it, so it runs once per
use rather than once per column.

A GeoArrow type carries its CRS, and Arrow compares that when it looks
for a kernel, so `crs` has to match the data. Pass every CRS you need in
one call: registering a name again replaces the kernels it had.

Loading geoarrowrs calls this once for you with `crs = NULL`, so data
with no CRS works without any setup. Data that carries one still needs a
call naming it. Set `options(geoarrowrs.register_udfs = FALSE)` before
loading to skip that, which also stops the package pulling in arrow at
load time.

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
