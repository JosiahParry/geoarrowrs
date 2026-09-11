# Buffer geometries by a given distance

Expands each geometry outward by `distance` to produce a multipolygon.
Line cap and join styles control the shape of the buffer at endpoints
and corners.

## Usage

``` r
ga_buffer(
  geometry,
  distance,
  line_cap = "round",
  line_join = "round",
  miter_limit = 2,
  round_angle = 0.2
)
```

## Arguments

- geometry:

  a GeoArrow geometry array

- distance:

  a numeric vector of buffer distances; length 1 or the same length as
  `geometry`

- line_cap:

  one of `"round"`, `"square"`, or `"butt"`

- line_join:

  one of `"round"`, `"miter"`, or `"bevel"`

- miter_limit:

  the miter limit used when `line_join` is `"miter"`

- round_angle:

  the angular step in radians used to approximate curves when `line_cap`
  or `line_join` is `"round"`. Smaller is smoother

## Value

a GeoArrow multipolygon array

## References

[Buffer](https://docs.rs/geo/latest/geo/algorithm/buffer/trait.Buffer.html)

## See also

Other misc:
[`ga_centroid()`](https://josiahparry.github.io/geoarrowrs/reference/ga_centroid.md),
[`ga_chaikin_smoothing()`](https://josiahparry.github.io/geoarrowrs/reference/ga_chaikin_smoothing.md),
[`ga_line_segmentize()`](https://josiahparry.github.io/geoarrowrs/reference/ga_line_segmentize.md),
[`ga_remove_repeated_points()`](https://josiahparry.github.io/geoarrowrs/reference/ga_remove_repeated_points.md)

## Examples

``` r
sq <- geoarrow::as_geoarrow_array(sf::st_sfc(
  sf::st_polygon(list(rbind(c(0, 0), c(2, 0), c(2, 2), c(0, 2), c(0, 0))))
))

# the 2x2 square grows by 8 along its sides plus a unit circle at the corners
as.vector(ga_unsigned_area(ga_buffer(sq, 1)))
#> [1] 15.12145

# a smaller angular step rounds the corners more finely
as.vector(ga_unsigned_area(ga_buffer(sq, 1, round_angle = 0.01)))
#> [1] 15.14108

# square corners instead
as.vector(ga_unsigned_area(ga_buffer(sq, 1, line_join = "bevel")))
#> [1] 14
```
