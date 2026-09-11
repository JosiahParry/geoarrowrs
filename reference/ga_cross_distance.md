# Distance from every row of `x` to every row of `y`

Measures the full cross product rather than walking the two arrays in
lockstep, giving one list of `length(y)` distances per row of `x`.

## Usage

``` r
ga_cross_distance(x, y, metric = "euclidean")
```

## Arguments

- x:

  a GeoArrow geometry array for `metric = "euclidean"`, a point array
  for the others

- y:

  a GeoArrow array matching `x`

- metric:

  one of `"euclidean"`, `"haversine"`, `"geodesic"`, `"rhumb"`, or
  `"vincenty"`

## Value

a list array with one element per row of `x`, each holding `length(y)`
distances

## Details

This is the shape
[`ga_dist_euclidean_pairwise()`](https://josiahparry.github.io/geoarrowrs/reference/dist_pairwise.md)
cannot express, and it costs `length(x) * length(y)` distances to hold:
ten thousand rows against ten thousand is a hundred million doubles, or
eight hundred megabytes. Where the distances are only wanted to pick a
nearest row or a threshold,
[`ga_sparse_knn()`](https://josiahparry.github.io/geoarrowrs/reference/ga_sparse_knn.md)
and
[`ga_sparse_dwithin()`](https://josiahparry.github.io/geoarrowrs/reference/ga_sparse_dwithin.md)
answer that against an R-tree without ever forming the product.

`"euclidean"` measures between geometries of any type. The spherical and
ellipsoidal metrics take points only, because `geo` defines them between
points alone. `"vincenty"` gives a null where the algorithm fails to
converge.

A null row of `x` gives a null element. A null row of `y` gives a null
in that position of every element.

## See also

Other distance:
[`ga_dist_euclidean_pairwise()`](https://josiahparry.github.io/geoarrowrs/reference/dist_pairwise.md),
[`ga_dist_frechet_pairwise()`](https://josiahparry.github.io/geoarrowrs/reference/ga_dist_frechet_pairwise.md),
[`ga_dist_hausdorff_pairwise()`](https://josiahparry.github.io/geoarrowrs/reference/ga_dist_hausdorff_pairwise.md),
[`ga_dist_vincenty_pairwise()`](https://josiahparry.github.io/geoarrowrs/reference/ga_dist_vincenty_pairwise.md)

## Examples

``` r
x <- ga_xy(c(-78.6382, -80.8431), c(35.7796, 35.2271))
y <- ga_xy(c(-77.9447, -78.6382, -80.8431), c(34.2257, 35.7796, 35.2271))

# two elements of three distances each, in meters
ga_cross_distance(x, y, "haversine")
#> <nanoarrow_array list[2]>
#>  $ length    : int 2
#>  $ null_count: int 0
#>  $ offset    : int 0
#>  $ buffers   :List of 2
#>   ..$ :<nanoarrow_buffer validity<bool>[null] ``
#>   ..$ :<nanoarrow_buffer data_offset<int32>[3][12 b]> `0 3 6`
#>  $ children  :List of 1
#>   ..$ item:<nanoarrow_array double[6]>
#>   .. ..$ length    : int 6
#>   .. ..$ null_count: int 0
#>   .. ..$ offset    : int 0
#>   .. ..$ buffers   :List of 2
#>   .. .. ..$ :<nanoarrow_buffer validity<bool>[null] ``
#>   .. .. ..$ :<nanoarrow_buffer data<double>[6][48 b]> `183968 0 208827 28731...`
#>   .. ..$ dictionary: NULL
#>   .. ..$ children  : list()
#>  $ dictionary: NULL
```
