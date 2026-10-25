# Keep the rows of a data frame that relate to another spatially

Filters `x` down to the rows whose geometry relates to any row of `y`.
Unlike
[`ga_join()`](https://josiahparry.github.io/geoarrowrs/reference/ga_join.md),
it does not attach the columns of `y`.

## Usage

``` r
ga_filter(x, y, predicate = ga_sparse_intersects, ...)
```

## Arguments

- x, y:

  data frames, each with one GeoArrow geometry column

- predicate:

  a sparse predicate, by default
  [`ga_sparse_intersects()`](https://josiahparry.github.io/geoarrowrs/reference/sparse.md)

## Value

an Arrow table holding the rows of `x` that matched

## Details

`ga_filter()` keeps a row of `x` when the predicate finds at least one
match for it in `y`, so `ga_filter(sites, counties, ga_sparse_within)`
keeps the sites that fall in any county. `ga_filter()` matches
`ST_Filter()`, and returns the same rows as
`ga_join(x, y, predicate, left = FALSE)` would, without the columns of
`y` and without repeating rows of `x`.

## See also

Other topology:
[`ga_contains()`](https://josiahparry.github.io/geoarrowrs/reference/topology.md),
[`ga_coordinate_position()`](https://josiahparry.github.io/geoarrowrs/reference/ga_coordinate_position.md),
[`ga_dimension()`](https://josiahparry.github.io/geoarrowrs/reference/ga_dimension.md),
[`ga_equals_exact()`](https://josiahparry.github.io/geoarrowrs/reference/ga_equals_exact.md),
[`ga_is_empty()`](https://josiahparry.github.io/geoarrowrs/reference/ga_is_empty.md),
[`ga_join()`](https://josiahparry.github.io/geoarrowrs/reference/ga_join.md),
[`ga_relate()`](https://josiahparry.github.io/geoarrowrs/reference/ga_relate.md),
[`ga_sparse_equals_exact()`](https://josiahparry.github.io/geoarrowrs/reference/ga_sparse_equals_exact.md),
[`ga_sparse_intersects()`](https://josiahparry.github.io/geoarrowrs/reference/sparse.md),
[`ga_sparse_pairs()`](https://josiahparry.github.io/geoarrowrs/reference/ga_sparse_pairs.md)

## Examples

``` r
nc <- as.data.frame(read_shapefile(
  system.file("shape/nc.shp", package = "sf")
))
sites <- data.frame(
  site = c("a", "b", "c"),
  geometry = geoarrow::as_geoarrow_vctr(
    ga_point(c(-78.6, -80.8, 0), c(35.8, 35.2, 0))
  )
)

# the third site is in the Atlantic, so ga_filter() drops it
ga_filter(sites, nc, ga_sparse_within)
#> Table
#> 2 rows x 2 columns
#> $site <string>
#> $geometry: geoarrow.point <crs <unspecified>>
#> 
#> See $metadata for additional Schema metadata
```
