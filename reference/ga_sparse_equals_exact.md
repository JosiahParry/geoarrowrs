# Find which rows of `y` are identical to each row of `x` within a tolerance

Returns the row numbers of `y` that have the same type and structure as
each row of `x`, with every coordinate within `tolerance` of its
counterpart. This is the sparse form of
[`ga_equals_exact()`](https://josiahparry.github.io/geoarrowrs/reference/ga_equals_exact.md),
and matches
[`sf::st_equals_exact()`](https://r-spatial.github.io/sf/reference/geos_binary_pred.html).

## Usage

``` r
ga_sparse_equals_exact(x, y, tolerance = 0)
```

## Arguments

- x:

  a GeoArrow geometry array

- y:

  a GeoArrow geometry array

- tolerance:

  the largest planar distance allowed between matching coordinates;
  length 1 or the same length as `x`

## Value

a list array of 1 based row numbers into `y`, the same length as `x`

## Details

`ga_sparse_equals_exact()` indexes `y` in a packed Hilbert R-tree and
compares only the rows whose bounding boxes come within `tolerance` of
the box of the row of `x`. It compares coordinates in order, so it does
not match a line to its reverse; use
[`ga_sparse_equals_topo()`](https://josiahparry.github.io/geoarrowrs/reference/sparse.md)
for that.

`ga_sparse_equals_exact()` returns a zero length element for a row of
`x` that matches nothing. It returns a null element for a null or empty
geometry in `x`, and for a `tolerance` that is `NA` or negative.

## See also

Other topology:
[`ga_contains()`](https://josiahparry.github.io/geoarrowrs/reference/topology.md),
[`ga_coordinate_position()`](https://josiahparry.github.io/geoarrowrs/reference/ga_coordinate_position.md),
[`ga_dimension()`](https://josiahparry.github.io/geoarrowrs/reference/ga_dimension.md),
[`ga_equals_exact()`](https://josiahparry.github.io/geoarrowrs/reference/ga_equals_exact.md),
[`ga_filter()`](https://josiahparry.github.io/geoarrowrs/reference/ga_filter.md),
[`ga_is_empty()`](https://josiahparry.github.io/geoarrowrs/reference/ga_is_empty.md),
[`ga_join()`](https://josiahparry.github.io/geoarrowrs/reference/ga_join.md),
[`ga_relate()`](https://josiahparry.github.io/geoarrowrs/reference/ga_relate.md),
[`ga_sparse_intersects()`](https://josiahparry.github.io/geoarrowrs/reference/sparse.md),
[`ga_sparse_pairs()`](https://josiahparry.github.io/geoarrowrs/reference/ga_sparse_pairs.md)

## Examples

``` r
nc <- as.data.frame(read_shapefile(
  system.file("shape/nc.shp", package = "sf")
))
shifted <- ga_translate(nc$geometry, 0.001, 0)

# each county matches its shifted copy once the tolerance covers the shift
as.vector(ga_sparse_equals_exact(nc$geometry, shifted, 0))[1:3]
#> <list_of<double>[3]>
#> [[1]]
#> numeric(0)
#> 
#> [[2]]
#> numeric(0)
#> 
#> [[3]]
#> numeric(0)
#> 
as.vector(ga_sparse_equals_exact(nc$geometry, shifted, 0.01))[1:3]
#> <list_of<double>[3]>
#> [[1]]
#> [1] 1
#> 
#> [[2]]
#> [1] 2
#> 
#> [[3]]
#> [1] 3
#> 
```
