# Collect an array into one geometry, or one per group

Gathers the geometries into a single multi geometry or geometry
collection, returning an array of length 1, or one per group when you
group the rows with `sizes` or `by`. This is the aggregate a
`summarise()` wants, not a row by row operation.

## Usage

``` r
ga_collect_agg(geometry, sizes = NULL, by = NULL)
```

## Arguments

- geometry:

  a GeoArrow geometry array

- sizes:

  the rows in each group, in order, or `NULL` for one group

- by:

  a character vector, numeric vector, or Arrow array with one label per
  row of `geometry`, or `NULL` to use `sizes` instead

## Value

a GeoArrow multipoint, multilinestring, multipolygon, or geometry
collection array, of length 1 or of one element per group

## Details

`ga_collect_agg()` picks the result type from the type of `geometry`, as
PostGIS `ST_Collect()` and
[`sf::st_combine()`](https://r-spatial.github.io/sf/reference/geos_combine.html)
do. It gathers a point or multipoint array into multipoints, a
linestring or multilinestring array into multilinestrings, and a
polygon, multipolygon, or box array into multipolygons. It gathers a
mixed geometry, WKB, or WKT array into geometry collections, since those
types can hold any geometry; call
[`ga_downcast_geometry()`](https://josiahparry.github.io/geoarrowrs/reference/ga_downcast_geometry.md)
first to get the multi type when every row has one kind.

`ga_collect_agg()` neither dissolves nor reorders, so it keeps
overlapping parts overlapping and keeps every part of every non null
input row. Use
[`ga_unary_union()`](https://josiahparry.github.io/geoarrowrs/reference/ga_unary_union.md)
to merge overlapping polygons into one outline instead.

`ga_collect_agg()` reads `sizes` as the number of rows in each group, in
order, which you get from a `group_by()` and `summarise(n = n())` over
the same ordering. You have to sort the rows by the grouping first,
since `ga_collect_agg()` takes the runs in row order: it returns the
`i`th run of `sizes` rows as group `i`. It returns nothing about the
groups themselves, so keep the keys from the same `summarise()` to say
what each row is.

With `by`, `ga_collect_agg()` groups the rows itself, from an unsorted
key of one label per row, so you need no `arrange()` first. It returns
one geometry per distinct label, in the order each label first appears
in `by`; get the matching keys back with `vctrs::vec_unique(by)`.
`ga_collect_agg()` groups rows with `NA` in `by` together like any other
label. Give at most one of `sizes` or `by`; with `NULL` for both, it
collects everything as one group.

Being an aggregate, this is one of the few functions here that does not
preserve length, and it has no Arrow kernel: a kernel sees one batch at
a time and would collect each batch separately.

## See also

Other aggregate:
[`ga_interpolate_aw()`](https://josiahparry.github.io/geoarrowrs/reference/ga_interpolate_aw.md),
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

# group by an unsorted key, without an arrange() first
key <- rep(c("north", "south"), length.out = pts$length)
by_key <- ga_collect_agg(pts, by = key)
vctrs::vec_unique(key)
#> [1] "north" "south"
```
