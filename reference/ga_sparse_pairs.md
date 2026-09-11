# Expand a sparse predicate into the row pairs a join needs

Turns the list of matches each sparse predicate returns into two
columns, one row per pair, which is the shape
[`ga_join()`](https://josiahparry.github.io/geoarrowrs/reference/ga_join.md)
takes and the shape a database returns a spatial join in.

## Usage

``` r
ga_sparse_pairs(hits, left = FALSE)
```

## Arguments

- hits:

  a list array from one of the sparse predicates

- left:

  whether to keep the rows of `x` that matched nothing

## Value

a struct array of `x` and `y` row numbers

## Details

Everything but the padding is expressible with Arrow's own
`list_flatten()` and `list_parent_indices()`. What they cannot do is
`left = TRUE`, which has to put back a row for each row of `x` that
matched nothing, so the whole expansion happens here rather than half
here and half in R.

Rows are 1 based, matching what the sparse predicates return. A row of
`x` that matched nothing appears once with a null `y` when
`left = TRUE`, and not at all otherwise.

## See also

Other topology:
[`ga_contains()`](https://josiahparry.github.io/geoarrowrs/reference/topology.md),
[`ga_coordinate_position()`](https://josiahparry.github.io/geoarrowrs/reference/ga_coordinate_position.md),
[`ga_dimension()`](https://josiahparry.github.io/geoarrowrs/reference/ga_dimension.md),
[`ga_filter()`](https://josiahparry.github.io/geoarrowrs/reference/ga_filter.md),
[`ga_is_empty()`](https://josiahparry.github.io/geoarrowrs/reference/ga_is_empty.md),
[`ga_join()`](https://josiahparry.github.io/geoarrowrs/reference/ga_join.md),
[`ga_relate()`](https://josiahparry.github.io/geoarrowrs/reference/ga_relate.md),
[`ga_sparse_intersects()`](https://josiahparry.github.io/geoarrowrs/reference/sparse.md)

## Examples

``` r
x <- ga_xy(c(0, 5, 1), c(0, 5, 1))
y <- ga_xy(c(0, 1), c(0, 1))

# the middle row of x matches nothing, so it is only there with left = TRUE
ga_sparse_pairs(ga_sparse_intersects(x, y))
#> <nanoarrow_array struct[2]>
#>  $ length    : int 2
#>  $ null_count: int 0
#>  $ offset    : int 0
#>  $ buffers   :List of 1
#>   ..$ :<nanoarrow_buffer validity<bool>[null] ``
#>  $ children  :List of 2
#>   ..$ x:<nanoarrow_array uint32[2]>
#>   .. ..$ length    : int 2
#>   .. ..$ null_count: int 0
#>   .. ..$ offset    : int 0
#>   .. ..$ buffers   :List of 2
#>   .. .. ..$ :<nanoarrow_buffer validity<bool>[null] ``
#>   .. .. ..$ :<nanoarrow_buffer data<uint32>[2][8 b]> `1 3`
#>   .. ..$ dictionary: NULL
#>   .. ..$ children  : list()
#>   ..$ y:<nanoarrow_array uint32[2]>
#>   .. ..$ length    : int 2
#>   .. ..$ null_count: int 0
#>   .. ..$ offset    : int 0
#>   .. ..$ buffers   :List of 2
#>   .. .. ..$ :<nanoarrow_buffer validity<bool>[null] ``
#>   .. .. ..$ :<nanoarrow_buffer data<uint32>[2][8 b]> `1 2`
#>   .. ..$ dictionary: NULL
#>   .. ..$ children  : list()
#>  $ dictionary: NULL
ga_sparse_pairs(ga_sparse_intersects(x, y), left = TRUE)
#> <nanoarrow_array struct[3]>
#>  $ length    : int 3
#>  $ null_count: int 0
#>  $ offset    : int 0
#>  $ buffers   :List of 1
#>   ..$ :<nanoarrow_buffer validity<bool>[null] ``
#>  $ children  :List of 2
#>   ..$ x:<nanoarrow_array uint32[3]>
#>   .. ..$ length    : int 3
#>   .. ..$ null_count: int 0
#>   .. ..$ offset    : int 0
#>   .. ..$ buffers   :List of 2
#>   .. .. ..$ :<nanoarrow_buffer validity<bool>[null] ``
#>   .. .. ..$ :<nanoarrow_buffer data<uint32>[3][12 b]> `1 2 3`
#>   .. ..$ dictionary: NULL
#>   .. ..$ children  : list()
#>   ..$ y:<nanoarrow_array uint32[3]>
#>   .. ..$ length    : int 3
#>   .. ..$ null_count: int 1
#>   .. ..$ offset    : int 0
#>   .. ..$ buffers   :List of 2
#>   .. .. ..$ :<nanoarrow_buffer validity<bool>[8][1 b]> `TRUE FALSE TRUE FALS...`
#>   .. .. ..$ :<nanoarrow_buffer data<uint32>[3][12 b]> `1 0 2`
#>   .. ..$ dictionary: NULL
#>   .. ..$ children  : list()
#>  $ dictionary: NULL
```
