library(sf)
library(arrow)
library(geoarrow)

xx <- open_dataset("inst/extdata/nc.parquet") |> 
  dplyr::collect()

# get the geoarrow geometry
g <- xx$geom |> 
  sf::st_as_sfc() |> 
  sf::st_cast("POLYGON") |> 
  sf::st_cast("LINESTRING") |>
  # geoarrow::as_geoarrow_array_stream()
  geoarrow::as_geoarrow_array()

# determine the schema
s <- nanoarrow::infer_nanoarrow_schema(g)

# find the length return array and schema as pointers
res <- length_euclidean_(g, s)

# add classes
a <- structure(res[[1]], class = "nanoarrow_array")
s <- structure(res[[2]], class = "nanoarrow_schema")

# set the array
nanoarrow::nanoarrow_array_set_schema(a, s)


arrow::as_arrow_array(a)

