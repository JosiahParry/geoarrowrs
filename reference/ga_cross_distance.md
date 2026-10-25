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

`ga_cross_distance()` returns the shape
[`ga_dist_euclidean_pairwise()`](https://josiahparry.github.io/geoarrowrs/reference/dist_pairwise.md)
cannot express, at the cost of holding `length(x) * length(y)`
distances: ten thousand rows against ten thousand is a hundred million
doubles, or eight hundred megabytes. Where you only want the distances
to pick a nearest row or a threshold,
[`ga_sparse_knn()`](https://josiahparry.github.io/geoarrowrs/reference/ga_sparse_knn.md)
and
[`ga_sparse_dwithin()`](https://josiahparry.github.io/geoarrowrs/reference/ga_sparse_dwithin.md)
answer that against an R-tree without ever forming the product.

With `"euclidean"`, `ga_cross_distance()` measures between geometries of
any type. It takes points only for the spherical and ellipsoidal
metrics, because `geo` defines them between points alone. With
`"vincenty"`, it returns a null where the algorithm fails to converge.

`ga_cross_distance()` returns a null element for a null row of `x`. For
a null row of `y`, it returns a null in that position of every element.

## See also

Other distance:
[`ga_dist_euclidean_pairwise()`](https://josiahparry.github.io/geoarrowrs/reference/dist_pairwise.md),
[`ga_dist_frechet_pairwise()`](https://josiahparry.github.io/geoarrowrs/reference/ga_dist_frechet_pairwise.md),
[`ga_dist_hausdorff_pairwise()`](https://josiahparry.github.io/geoarrowrs/reference/ga_dist_hausdorff_pairwise.md),
[`ga_dist_vincenty_pairwise()`](https://josiahparry.github.io/geoarrowrs/reference/ga_dist_vincenty_pairwise.md)

## Examples

``` r
x <- ga_point(c(-78.6382, -80.8431), c(35.7796, 35.2271))
y <- ga_point(c(-77.9447, -78.6382, -80.8431), c(34.2257, 35.7796, 35.2271))

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
