devtools::load_all()

res <- read_geojson_("inst/extdata/osm-edinburgh-central.geojson", 6809)

sf::st_write(osm_edinburgh_central, "inst/extdata/osm-edinburgh-central.fgb")
sf::st_write(osm_edinburgh_central, "inst/extdata/shp/osm-edinburgh-central.shp")

res <- read_shapefile_("inst/extdata/shp/osm-edinburgh-central.shp") |>
  arrow::as_arrow_table() |>
  sf::st_as_sf()

osm_edinburgh_central |>
  as_tibble() |>
  write_parquet()

bench::mark(
  `geoarrow-r` = arrow::open_dataset("inst/extdata/osm-edinburgh-central.parquet") |>
    sf::st_as_sf(),
  read_geoparquet_(
    "inst/extdata/osm-edinburgh-central.parquet",
    batch_size = NA,
    NULL,
    # bbox = c(-3.2081305, 55.9500772, -3.1885133, 55.9534548),
    NA, NA, NULL
  ) |>
    arrow::as_record_batch_reader() |>
    sf::st_as_sf()
)



library(geoarrow)

bench::mark(
  sf = sf::st_read("inst/extdata/osm-edinburgh-central.fgb", quiet = TRUE),
  geoarrow = read_flatgeobuf_("inst/extdata/osm-edinburgh-central.fgb") |>
    arrow::as_arrow_table() |>
    sf::st_as_sf(),
  check = FALSE
)



# bbox range read
read_flatgeobuf_(
  "inst/extdata/osm-edinburgh-central.fgb",
  c(-3.2081305, 55.9500772, -3.1885133, 55.9534548)
) |>
  arrow::as_arrow_table() |>
  sf::st_as_sf()



# geoparquet writer
stream <- osm_edinburgh_central |>
  as_tibble() |>
  dplyr::mutate(geometry = as_geoarrow_vctr(geometry)) |>
  arrow::as_arrow_table() |>
  as_nanoarrow_array_stream()
