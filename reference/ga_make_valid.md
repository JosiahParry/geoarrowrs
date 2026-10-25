# Repair invalid polygons

Returns a valid multipolygon for each polygon, multipolygon, or box,
splitting a self intersecting ring at its crossings and removing
dangling edges and holes that cancel out. Use
[`ga_is_valid()`](https://josiahparry.github.io/geoarrowrs/reference/validation.md)
to find the rows that need it.

## Usage

``` r
ga_make_valid(geometry)
```

## Arguments

- geometry:

  a GeoArrow geometry array

## Value

a GeoArrow multipolygon array of the same length as `geometry`

## Details

`ga_make_valid()` uses the prepair algorithm of Ledoux, Arroyo Ohori,
and Meijers (2014): it triangulates the rings and keeps each triangle
that an odd number of ring edges separates from the outside. It always
returns a multipolygon, since one invalid polygon can split into several
valid parts.

The odd-even rule gives the same shape as
[`sf::st_make_valid()`](https://r-spatial.github.io/sf/reference/valid.html)
for a self intersecting ring, a dangling spike, and a hole that touches
or leaves its shell, but not where rings overlap. sf's default GEOS
method subtracts the union of the holes and unions overlapping parts,
while `ga_make_valid()` keeps the overlap of two holes and a hole inside
a hole as islands, and drops the overlap of two parts.

`ga_make_valid()` returns an empty multipolygon for an empty polygon. It
returns a null for a null geometry, for a geometry that is not
polygonal, and for a polygon it cannot repair, such as one with a `NaN`
or infinite coordinate.

## References

[MakeValid](https://docs.rs/geo/latest/geo/algorithm/repair_polygon/trait.MakeValid.html)

## See also

Other validation:
[`ga_is_valid()`](https://josiahparry.github.io/geoarrowrs/reference/validation.md)

## Examples

``` r
bowtie <- rbind(c(0, 0), c(2, 2), c(2, 0), c(0, 2), c(0, 0))
g <- geoarrow::as_geoarrow_array(sf::st_sfc(sf::st_polygon(list(bowtie))))
as.vector(ga_is_valid(g))
#> [1] FALSE

# the crossing splits the bowtie into two triangles
fixed <- ga_make_valid(g)
sf::st_as_sfc(geoarrow::as_geoarrow_vctr(fixed))
#> Geometry set for 1 feature 
#> Geometry type: MULTIPOLYGON
#> Dimension:     XY
#> Bounding box:  xmin: 0 ymin: 0 xmax: 2 ymax: 2
#> CRS:           NA
#> MULTIPOLYGON (((2 2, 1 1, 2 0, 2 2)), ((1 1, 0 ...
as.vector(ga_is_valid(fixed))
#> [1] TRUE
```
