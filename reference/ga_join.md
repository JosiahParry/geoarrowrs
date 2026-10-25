# Join two data frames on a spatial relationship

Attaches the columns of `y` to each row of `x` that it relates to, the
way [`merge()`](https://rdrr.io/r/base/merge.html) attaches them on a
shared key. `ga_join()` keeps the geometry of `x` and drops the geometry
of `y`.

## Usage

``` r
ga_join(
  x,
  y,
  predicate = ga_sparse_intersects,
  ...,
  suffix = c("_x", "_y"),
  left = TRUE
)
```

## Arguments

- x, y:

  data frames, each with one GeoArrow geometry column

- predicate:

  a sparse predicate, by default
  [`ga_sparse_intersects()`](https://josiahparry.github.io/geoarrowrs/reference/sparse.md)

- suffix:

  the pair of suffixes to add to the column names both frames share

- left:

  whether to keep rows of `x` that match nothing

## Value

an Arrow table with the columns of `x` followed by the non geometry
columns of `y`

## Details

`ga_join()` repeats a row of `x` once for each row of `y` it matches, so
the result is usually longer than `x`. With `left = TRUE` it keeps a row
that matches nothing once, with `NA` in every column from `y`; with
`left = FALSE` it drops that row.

`predicate` is any of the sparse predicates, such as
[`ga_sparse_within()`](https://josiahparry.github.io/geoarrowrs/reference/sparse.md)
or
[`ga_sparse_touches()`](https://josiahparry.github.io/geoarrowrs/reference/sparse.md).
`ga_join()` calls it once as `predicate(x_geometry, y_geometry)`, so use
[`ga_sparse_within()`](https://josiahparry.github.io/geoarrowrs/reference/sparse.md)
for points in polygons and
[`ga_sparse_contains()`](https://josiahparry.github.io/geoarrowrs/reference/sparse.md)
for polygons holding points.

`ga_join()` suffixes the columns the two frames share rather than
overwriting them. Both frames need exactly one GeoArrow geometry column.

## See also

Other topology:
[`ga_contains()`](https://josiahparry.github.io/geoarrowrs/reference/topology.md),
[`ga_coordinate_position()`](https://josiahparry.github.io/geoarrowrs/reference/ga_coordinate_position.md),
[`ga_dimension()`](https://josiahparry.github.io/geoarrowrs/reference/ga_dimension.md),
[`ga_equals_exact()`](https://josiahparry.github.io/geoarrowrs/reference/ga_equals_exact.md),
[`ga_filter()`](https://josiahparry.github.io/geoarrowrs/reference/ga_filter.md),
[`ga_is_empty()`](https://josiahparry.github.io/geoarrowrs/reference/ga_is_empty.md),
[`ga_relate()`](https://josiahparry.github.io/geoarrowrs/reference/ga_relate.md),
[`ga_sparse_equals_exact()`](https://josiahparry.github.io/geoarrowrs/reference/ga_sparse_equals_exact.md),
[`ga_sparse_intersects()`](https://josiahparry.github.io/geoarrowrs/reference/sparse.md),
[`ga_sparse_pairs()`](https://josiahparry.github.io/geoarrowrs/reference/ga_sparse_pairs.md)

## Examples

``` r
nc <- as.data.frame(read_shapefile(
  system.file("shape/nc.shp", package = "sf")
))
counties <- nc[c("NAME", "geometry")]
sites <- data.frame(
  site = c("a", "b"),
  geometry = geoarrow::as_geoarrow_vctr(
    ga_point(c(-78.6, -80.8), c(35.8, 35.2))
  )
)

# which county each site falls in
ga_join(sites, counties, ga_sparse_within)
#> Table
#> 2 rows x 3 columns
#> $site <string>
#> $geometry: geoarrow.point <crs <unspecified>>
#> $NAME <string>

# every pair of neighbouring counties
head(ga_join(counties, counties, ga_sparse_touches)[c("NAME_x", "NAME_y")], 3)
#> Table
#> 3 rows x 2 columns
#> $NAME_x <string>
#> $NAME_y <string>
```
