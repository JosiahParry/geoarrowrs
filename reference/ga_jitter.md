# Move each geometry by a small random offset

Shifts each geometry of `x` as a whole by a random distance along x and
along y, each drawn uniformly between `-amount` and `amount`. This
matches
[`sf::st_jitter()`](https://r-spatial.github.io/sf/reference/st_jitter.html)
in everything but the random numbers.

## Usage

``` r
ga_jitter(
  x,
  amount = NULL,
  factor = 0.002,
  seed = sample.int(.Machine$integer.max, 1L)
)
```

## Arguments

- x:

  a GeoArrow geometry array

- amount:

  the most to move a geometry along each axis; length 1 or the same
  length as `x`, or `NULL` to take it from `factor`

- factor:

  the fraction of the diagonal of the box around `x` to move a geometry
  by at most when you give no `amount`

- seed:

  a whole number that seeds the offsets

## Value

a GeoArrow array of the same geometry type and length as `x`

## Details

Without `amount`, `ga_jitter()` uses `factor` times the length of the
diagonal of the box around all of `x`, as sf does. `ga_jitter()` treats
coordinates as planar.

`ga_jitter()` draws the offsets from its own generator, seeded by
`seed`, two for every row in order. The default seed comes from R's
generator, so [`set.seed()`](https://rdrr.io/r/base/Random.html) makes
the result repeatable, but `ga_jitter()` never draws the numbers sf
would.

`ga_jitter()` keeps the geometry type of `x`. It returns null for a null
geometry, and for an `amount` that is `NA`, negative, or infinite.

## See also

Other affine:
[`ga_affine_transform()`](https://josiahparry.github.io/geoarrowrs/reference/ga_affine_transform.md),
[`ga_rotate_around_center()`](https://josiahparry.github.io/geoarrowrs/reference/ga_rotate_around_center.md),
[`ga_rotate_around_centroid()`](https://josiahparry.github.io/geoarrowrs/reference/ga_rotate_around_centroid.md),
[`ga_scale_xy()`](https://josiahparry.github.io/geoarrowrs/reference/ga_scale_xy.md),
[`ga_skew()`](https://josiahparry.github.io/geoarrowrs/reference/ga_skew.md),
[`ga_skew_xy()`](https://josiahparry.github.io/geoarrowrs/reference/ga_skew_xy.md),
[`ga_translate()`](https://josiahparry.github.io/geoarrowrs/reference/ga_translate.md)

## Examples

``` r
pts <- ga_point(c(0, 10, 10), c(0, 0, 10))

# each point moves by at most 0.5 along each axis
moved <- ga_jitter(pts, amount = 0.5, seed = 1)
as.vector(ga_x(moved)) - c(0, 10, 10)
#> [1]  0.3398737 -0.3860983  0.3883953

# the same seed moves each point the same way
identical(
  as.vector(ga_x(ga_jitter(pts, seed = 42))),
  as.vector(ga_x(ga_jitter(pts, seed = 42)))
)
#> [1] TRUE
```
