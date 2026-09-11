# Set the thread cap, from `options(geoarrowrs.thread_pool = )`

Called on load and whenever the option changes. `0` returns the parallel
paths to rayon's own default, which is one thread per core.

## Usage

``` r
ga_set_thread_pool(n)
```

## Arguments

- n:

  the most threads to use, or `0` for rayon's default

## Value

`n`, invisibly

## See also

Other index:
[`KDTree`](https://josiahparry.github.io/geoarrowrs/reference/KDTree.md),
[`RTree`](https://josiahparry.github.io/geoarrowrs/reference/RTree.md),
[`ga_envelope()`](https://josiahparry.github.io/geoarrowrs/reference/ga_envelope.md),
[`ga_knn_join()`](https://josiahparry.github.io/geoarrowrs/reference/ga_knn_join.md),
[`ga_sparse_dwithin()`](https://josiahparry.github.io/geoarrowrs/reference/ga_sparse_dwithin.md),
[`ga_sparse_knn()`](https://josiahparry.github.io/geoarrowrs/reference/ga_sparse_knn.md)

## Examples

``` r
# hold the parallel paths to two threads
ga_set_thread_pool(2)
#> [1] 2

# and back to one per core
ga_set_thread_pool(0)
#> [1] 0
```
