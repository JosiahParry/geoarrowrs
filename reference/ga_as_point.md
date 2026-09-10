# Cast to one GeoArrow geometry type

Each of these casts to the single type its name gives. Because the
result type does not depend on the data, these are the casts Arrow can
run inside a query, which
[`ga_from_wkb()`](https://josiahparry.github.io/geoarrowrs/reference/ga_from_wkb.md)
cannot.

## Usage

``` r
ga_as_point(x)

ga_as_linestring(x)

ga_as_polygon(x)

ga_as_multipoint(x)

ga_as_multilinestring(x)

ga_as_multipolygon(x)
```

## Arguments

- x:

  a binary array of WKB, or any GeoArrow array

## Value

a GeoArrow array of the named type, the same length as `x`

## Details

Parsing WKB is the expensive part of using a plain Parquet geometry
column, and every function that needs a concrete geometry type pays for
it on its own. Casting once up front means the rest of the query runs on
native GeoArrow instead:

    trip |>
      mutate(pickup = ga_as_point(t_pickuploc)) |>
      mutate(
        d = ga_dist_euclidean_pairwise(pickup, dropoff),
        b = ga_bearing_euclidean(pickup, dropoff)
      )

The cast is fallible. A geometry that does not fit the target type, such
as a two point multipoint cast to `point`, is an error rather than a
null.

## See also

Other cast:
[`ga_cast_geometry()`](https://josiahparry.github.io/geoarrowrs/reference/ga_cast_geometry.md),
[`ga_downcast_geometry()`](https://josiahparry.github.io/geoarrowrs/reference/ga_downcast_geometry.md),
[`ga_explode()`](https://josiahparry.github.io/geoarrowrs/reference/ga_explode.md),
[`ga_flatten()`](https://josiahparry.github.io/geoarrowrs/reference/ga_flatten.md),
[`ga_from_wkb()`](https://josiahparry.github.io/geoarrowrs/reference/ga_from_wkb.md)

## Examples

``` r
sfc <- sf::st_sfc(sf::st_point(c(0, 0)), sf::st_point(c(3, 4)))
wkb <- nanoarrow::as_nanoarrow_array(
  arrow::Array$create(unclass(wk::as_wkb(sfc)), type = arrow::binary())
)

pts <- ga_as_point(wkb)
sf::st_as_sfc(geoarrow::as_geoarrow_vctr(pts))
#> Geometry set for 2 features 
#> Geometry type: POINT
#> Dimension:     XY
#> Bounding box:  xmin: 0 ymin: 0 xmax: 3 ymax: 4
#> CRS:           NA
#> POINT (0 0)
#> POINT (3 4)
```
