# Collect an array into one geometry, or one per group

Gathers the geometries into a single geometry collection, returning an
array of length 1, or one collection per group when `sizes` says how the
rows are grouped. This is the aggregate a `summarise()` wants, not a row
by row operation.

## Usage

``` r
ga_collect_agg(geometry, sizes = NULL)
```

## Arguments

- geometry:

  a GeoArrow geometry array

- sizes:

  the rows in each group, in order, or `NULL` for one group

## Value

a GeoArrow geometry collection array, of length 1 or of one element per
group

## Details

Nothing is dissolved and nothing is reordered, so overlapping parts stay
overlapping and the collection holds one member per non null input row.
Use
[`ga_unary_union()`](https://josiahparry.github.io/geoarrowrs/reference/ga_unary_union.md)
to merge overlapping polygons into one outline instead.

`sizes` is the number of rows in each group, in order, which is what a
`group_by()` and `summarise(n = n())` over the same ordering gives. The
rows have to already be sorted by the grouping, since the runs are taken
as they come: group `i` of the result is the `i`th run of `sizes` rows.
Nothing is returned about the groups themselves, so keep the keys from
the same `summarise()` to say what each row is. `NULL` collects
everything as one group.

Being an aggregate, this is one of the few functions here that does not
preserve length, and it is not registered as an Arrow kernel: a kernel
sees one batch at a time and would collect each batch separately.

## See also

Other aggregate:
[`ga_unary_union()`](https://josiahparry.github.io/geoarrowrs/reference/ga_unary_union.md)

## Examples

``` r
nc <- as.data.frame(read_shapefile(
  system.file("shape/nc.shp", package = "sf")
))
pts <- ga_centroid(nc$geometry)

# every centroid as one geometry, then the shape they span
collected <- ga_collect_agg(pts)
as.vector(ga_unsigned_area(ga_convex_hull(collected)))
#> [1] 13.3302

# the same, in groups of twenty rows
grouped <- ga_collect_agg(pts, sizes = rep(20, 5))
as.vector(ga_unsigned_area(ga_convex_hull(grouped)))
#> [1] 1.390484 1.798231 2.411533 2.197786 5.730774
```
