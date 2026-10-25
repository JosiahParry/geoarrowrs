# Add points so no segment exceeds a length

Densifies linestrings, multilinestrings, polygons, and multipolygons by
inserting additional points along each segment until no segment exceeds
`max_segment_length`. `ga_densify()` measures segment length with
`metric`.

## Usage

``` r
ga_densify(geometry, max_segment_length, metric)
```

## Arguments

- geometry:

  a GeoArrow linestring, multilinestring, polygon, or multipolygon array

- max_segment_length:

  the maximum segment length; either length 1 or the same length as
  `geometry`

- metric:

  one of `"euclidean"`, `"haversine"`, `"geodesic"`, or `"rhumb"`

## Value

a GeoArrow array of the same geometry type as the input

## References

[Densifiable](https://docs.rs/geo/latest/geo/algorithm/line_measures/trait.Densifiable.html)

## Examples

``` r
line <- geoarrow::as_geoarrow_array(sf::st_sfc(
  sf::st_linestring(rbind(c(0, 0), c(10, 0)))
))

# ga_densify() adds four new vertices to cap a 10 unit segment at 2 units
as.vector(ga_n_coords(line))
#> [1] 2
as.vector(ga_n_coords(ga_densify(line, 2, "euclidean")))
#> [1] 6
```
