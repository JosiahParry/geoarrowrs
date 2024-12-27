library(sf)
library(arrow)
library(geoarrow)
library(nanoarrow)

xx <- open_dataset("inst/extdata/nc.parquet") |>
  dplyr::collect()

# get the geoarrow geometry
g <- xx$geom |>
  sf::st_as_sfc() |>
  sf::st_cast("POLYGON") |>
  sf::st_cast("LINESTRING") |>
  geoarrow::as_geoarrow_vctr()

attributes(g)

h <- chunks_from_geoarrow_vctr(g)
h
g


attributes(h)
