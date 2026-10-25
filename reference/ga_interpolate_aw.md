# Interpolate values from one set of polygons onto another by area

Moves values that describe the polygons of `x` onto the polygons of
`to`, sharing each value out by the area the two overlap. This matches
[`sf::st_interpolate_aw()`](https://r-spatial.github.io/sf/reference/interpolate_aw.html).

## Usage

``` r
ga_interpolate_aw(
  x,
  values,
  to,
  extensive,
  include_non_intersected = FALSE,
  allocate_total = FALSE,
  na_rm = FALSE,
  weights = NULL
)
```

## Arguments

- x:

  a GeoArrow polygon array that the values describe

- values:

  the values of `x`, one row per row of `x` or a single row for all of
  them: a numeric vector or float array, or a data frame, list, or
  struct array of numeric columns

- to:

  a GeoArrow polygon array to interpolate onto

- extensive:

  `TRUE` to interpolate a column by preserving its total, `FALSE` to
  preserve its mean; length 1 or one per column of `values`

- include_non_intersected:

  whether to divide an intensive column by the whole area of each target
  rather than the part the sources cover

- allocate_total:

  whether to divide an extensive column by the part of each source the
  targets cover rather than its whole area

- na_rm:

  whether to drop each source with a null in any column of `values`

- weights:

  the weight of each row of `to`, to share extensive values out by in
  place of area, or `NULL`

## Value

a float64 array of the same length as `to`, or a struct array of float64
columns when `values` is a table

## Details

Areal interpolation needs to know how each value relates to the area it
describes, which you state with `extensive`. For a spatially extensive
value, such as a count of people, `ga_interpolate_aw()` gives each
target the share of the value in proportion to the area it takes from
the source, so it preserves the total. For a spatially intensive value,
such as a density, `ga_interpolate_aw()` gives each target the mean of
the values it overlaps, weighted by area, so it preserves the mean.

With `a` the area that source `i` shares with target `j`,
`ga_interpolate_aw()` returns the sum over `i` of `v[i] * a / A[i]` for
an extensive column, where `A[i]` is the area of the source, and of
`v[i] * a / B[j]` for an intensive column, where `B[j]` is the area of
the target that the sources cover. `allocate_total = TRUE` takes `A[i]`
as the area of the source that the targets cover, so
`ga_interpolate_aw()` hands out all of each value even where the targets
leave part of its source uncovered, as tobler's `area_interpolate()`
does by default. `include_non_intersected = TRUE` takes `B[j]` as the
whole area of the target, as sf's argument of that name does.

With `weights`, `ga_interpolate_aw()` shares each source out by weight
rather than by area, as sf does for dasymetric mapping. It spreads the
weight of each target over its overlaps by area, then gives each overlap
the fraction of its source's weight that it holds. `ga_interpolate_aw()`
takes `weights` for extensive columns only.

`ga_interpolate_aw()` measures planar area, so project geographic
coordinates first. It reads polygons, multipolygons, and boxes, and
treats any other geometry as covering nothing. It counts a source as
overlapping a target only when their intersection has area, so it
ignores polygons that share just an edge.

`ga_interpolate_aw()` returns null for a target that no source overlaps,
and for a target that a source with a null value overlaps. With
`na_rm = TRUE`, it first drops each source with a null in any column, as
sf's `na.rm` does. When `values` is a table, `ga_interpolate_aw()`
returns a struct array that is null in each row no source overlaps.

## References

Pebesma, E., Fleischmann, M., Parry, J., et al. (2025). Spatial data
science languages: commonalities and needs. *Journal of Spatial
Information Science*, 31, 119–144.
[doi:10.5311/JOSIS.2025.31.462](https://doi.org/10.5311/JOSIS.2025.31.462)

## See also

Other aggregate:
[`ga_collect_agg()`](https://josiahparry.github.io/geoarrowrs/reference/ga_collect_agg.md),
[`ga_unary_union()`](https://josiahparry.github.io/geoarrowrs/reference/ga_unary_union.md)

## Examples

``` r
nc <- sf::st_transform(
  sf::read_sf(system.file("shape/nc.shp", package = "sf")),
  32119
)
counties <- geoarrow::as_geoarrow_array(sf::st_geometry(nc))
cells <- ga_make_grid(counties, n = c(10, 5))

# births are a count, so the cells keep the state's total
births <- ga_interpolate_aw(counties, nc$BIR74, cells, extensive = TRUE)
sum(as.vector(births), na.rm = TRUE) / sum(nc$BIR74)
#> [1] 1

# a count and a rate together, each with its own support
both <- data.frame(births = nc$BIR74, sids_rate = nc$SID74 / nc$BIR74)
out <- ga_interpolate_aw(counties, both, cells, extensive = c(TRUE, FALSE))
head(na.omit(as.data.frame(out)))
#>        births   sids_rate
#> 6   531.58653 0.004002972
#> 7  4837.74162 0.003674742
#> 8  6487.63049 0.002492565
#> 14   30.75959 0.001021711
#> 15 2477.34775 0.005111840
#> 16 8683.01518 0.003622510
```
