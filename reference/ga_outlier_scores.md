# Score how much each point looks like an outlier

Returns the local outlier factor of every point in the array, one score
per row.

## Usage

``` r
ga_outlier_scores(geometry, k_neighbours)
```

## Arguments

- geometry:

  a GeoArrow point array

- k_neighbours:

  how many neighbours define a neighbourhood

## Value

a double array of scores, the same length as `geometry`

## Details

A score near 1 means a point sits at the same density as its neighbours.
Scores meaningfully above 1 mean it is in a sparser neighbourhood than
they are, which is what marks an outlier. There is no universal cutoff,
so compare scores within a dataset rather than against a fixed
threshold.

`k_neighbours` sets how many neighbours define the local neighbourhood.
Small values react to fine structure and large values smooth it away.
Scoring is over the whole array, not within each row, so it is a single
value. A row that is not a single point, or is null, comes back null.

## References

[OutlierDetection](https://docs.rs/geo/latest/geo/algorithm/outlier_detection/trait.OutlierDetection.html)

## See also

Other cluster:
[`ga_dbscan()`](https://josiahparry.github.io/geoarrowrs/reference/ga_dbscan.md),
[`ga_kmeans()`](https://josiahparry.github.io/geoarrowrs/reference/ga_kmeans.md)

## Examples

``` r
g <- ga_xy(
  c(0, 0.1, 0.2, 0.3, 9),
  c(0, 0.1, 0.2, 0.3, 9)
)

round(as.vector(ga_outlier_scores(g, k_neighbours = 2)), 2)
#> [1]    1    1    1    1 3785
```
