# Find which rows of `y` relate to each row of `x`

Each function compares every row of `x` against every row of `y` and
returns the row numbers of `y` for which the named DE-9IM relationship
holds. This is the sparse form of the pairwise predicates in
[`ga_intersects()`](https://josiahparry.github.io/geoarrowrs/reference/topology.md),
and the shape
[`ga_join()`](https://josiahparry.github.io/geoarrowrs/reference/ga_join.md)
needs.

## Usage

``` r
ga_sparse_intersects(x, y)

ga_sparse_contains(x, y)

ga_sparse_contains_properly(x, y)

ga_sparse_within(x, y)

ga_sparse_covers(x, y)

ga_sparse_covered_by(x, y)

ga_sparse_touches(x, y)

ga_sparse_crosses(x, y)

ga_sparse_overlaps(x, y)

ga_sparse_equals_topo(x, y)
```

## Arguments

- x:

  a GeoArrow geometry array

- y:

  a GeoArrow geometry array

## Value

a list array of 1 based row numbers into `y`, the same length as `x`

## Details

The comparison is not the full cross product. `y` is indexed in a packed
Hilbert R-tree and only the rows whose bounding box overlaps are relate
tested, so the cost scales with the number of candidates rather than
with `length(x) * length(y)`.

A row of `x` that matches nothing gives a zero length element, not a
null. A null or empty geometry in `x` gives a null element, and one in
`y` is never returned.

[`ga_disjoint()`](https://josiahparry.github.io/geoarrowrs/reference/topology.md)
has no sparse form. Disjointness is the one relationship a bounding box
cannot narrow, so the answer is almost every row of `y` and the result
is denser than the input.

## References

[Relate](https://docs.rs/geo/latest/geo/algorithm/relate/trait.Relate.html)

## See also

Other topology:
[`ga_contains()`](https://josiahparry.github.io/geoarrowrs/reference/topology.md),
[`ga_coordinate_position()`](https://josiahparry.github.io/geoarrowrs/reference/ga_coordinate_position.md),
[`ga_dimension()`](https://josiahparry.github.io/geoarrowrs/reference/ga_dimension.md),
[`ga_filter()`](https://josiahparry.github.io/geoarrowrs/reference/ga_filter.md),
[`ga_is_empty()`](https://josiahparry.github.io/geoarrowrs/reference/ga_is_empty.md),
[`ga_join()`](https://josiahparry.github.io/geoarrowrs/reference/ga_join.md),
[`ga_relate()`](https://josiahparry.github.io/geoarrowrs/reference/ga_relate.md),
[`ga_sparse_pairs()`](https://josiahparry.github.io/geoarrowrs/reference/ga_sparse_pairs.md)

## Examples

``` r
nc <- as.data.frame(read_shapefile(
  system.file("shape/nc.shp", package = "sf")
))

# which counties each county touches
nbrs <- as.vector(ga_sparse_touches(nc$geometry, nc$geometry))
nbrs[[1]]
#> [1]  2 18 19

# how many neighbours each has
summary(lengths(nbrs))
#>    Min. 1st Qu.  Median    Mean 3rd Qu.    Max. 
#>     2.0     4.0     5.0     4.9     6.0     9.0 
```
