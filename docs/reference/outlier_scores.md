# Score how much each point looks like an outlier

Returns the local outlier factor of every point in a geometry, one list
per input geometry.

## Usage

``` r
outlier_scores(geometry, k_neighbours)
```

## Arguments

- geometry:

  a GeoArrow multipoint array

- k_neighbours:

  how many neighbours define a neighbourhood; length 1 or the same
  length as `geometry`

## Value

a list array of doubles, one list per input geometry

## Details

A score near 1 means a point sits at the same density as its neighbours.
Scores meaningfully above 1 mean it is in a sparser neighbourhood than
they are, which is what marks an outlier. There is no universal cutoff,
so compare scores within a dataset rather than against a fixed
threshold.

`k_neighbours` sets how many neighbours define the local neighbourhood.
Small values react to fine structure and large values smooth it away. A
row that is not point based, or is null, comes back null.

## References

[OutlierDetection](https://docs.rs/geo/latest/geo/algorithm/outlier_detection/trait.OutlierDetection.html)

## See also

Other cluster:
[`dbscan()`](https://josiahparry.github.io/geoarrowrs/reference/dbscan.md),
[`kmeans()`](https://josiahparry.github.io/geoarrowrs/reference/kmeans.md)

## Examples

``` r
pts <- sf::st_multipoint(cbind(
  c(0, 0.1, 0.2, 0.3, 9),
  c(0, 0.1, 0.2, 0.3, 9)
))
g <- geoarrow::as_geoarrow_array(sf::st_sfc(pts))

nanoarrow::convert_array(outlier_scores(g, k_neighbours = 2))
#> <list_of<double>[1]>
#> [[1]]
#> [1]    1    1    1    1 3785
#> 
```
