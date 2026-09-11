# Join two data frames on a spatial relationship

Attaches the columns of `y` to each row of `x` that it relates to, the
way [`merge()`](https://rdrr.io/r/base/merge.html) attaches them on a
shared key. The geometry of `x` is kept and the geometry of `y` is
dropped.

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

  the pair of suffixes added to column names found in both frames

- left:

  whether to keep rows of `x` that match nothing

## Value

a data frame with the columns of `x` followed by the non geometry
columns of `y`

## Details

A row of `x` matching several rows of `y` is repeated once per match, so
the result is usually longer than `x`. With `left = TRUE` a row matching
nothing is kept once with `NA` in every column from `y`; with
`left = FALSE` it is dropped.

`predicate` is any of the sparse predicates, such as
[`ga_sparse_within()`](https://josiahparry.github.io/geoarrowrs/reference/sparse.md)
or
[`ga_sparse_touches()`](https://josiahparry.github.io/geoarrowrs/reference/sparse.md).
It is called once as `predicate(x_geometry, y_geometry)`, so points in
polygons is
[`ga_sparse_within()`](https://josiahparry.github.io/geoarrowrs/reference/sparse.md)
and polygons holding points is
[`ga_sparse_contains()`](https://josiahparry.github.io/geoarrowrs/reference/sparse.md).

Columns the two frames share are suffixed rather than overwritten. Both
frames need exactly one GeoArrow geometry column.

## See also

Other topology:
[`ga_contains()`](https://josiahparry.github.io/geoarrowrs/reference/topology.md),
[`ga_coordinate_position()`](https://josiahparry.github.io/geoarrowrs/reference/ga_coordinate_position.md),
[`ga_dimension()`](https://josiahparry.github.io/geoarrowrs/reference/ga_dimension.md),
[`ga_is_empty()`](https://josiahparry.github.io/geoarrowrs/reference/ga_is_empty.md),
[`ga_relate()`](https://josiahparry.github.io/geoarrowrs/reference/ga_relate.md),
[`ga_sparse_intersects()`](https://josiahparry.github.io/geoarrowrs/reference/sparse.md)

## Examples

``` r
nc <- as.data.frame(read_shapefile(
  system.file("shape/nc.shp", package = "sf")
))
counties <- nc[c("NAME", "geometry")]
sites <- data.frame(
  site = c("a", "b"),
  geometry = geoarrow::as_geoarrow_vctr(
    ga_xy(c(-78.6, -80.8), c(35.8, 35.2))
  )
)

# which county each site falls in
ga_join(sites, counties, ga_sparse_within)
#>   site             geometry        NAME
#> 1    a <POINT (-78.6 35.8)>        Wake
#> 2    b <POINT (-80.8 35.2)> Mecklenburg

# every pair of neighbouring counties
head(ga_join(counties, counties, ga_sparse_touches)[c("NAME_x", "NAME_y")], 3)
#>   NAME_x    NAME_y
#> 1   Ashe Alleghany
#> 2   Ashe    Wilkes
#> 3   Ashe   Watauga
```
