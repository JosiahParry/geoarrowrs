skip_if_not_installed("sf")
skip_if_not_installed("geoarrow")
skip_if_not_installed("yyjsonr")

# a projected crs written as PROJJSON, the way geoarrow writes it from sf
nc <- sf::st_geometry(sf::read_sf(system.file("shape/nc.shp", package = "sf")))
nc <- sf::st_transform(nc[1:5], 32119)
inputs <- list(
  points = geoarrow::as_geoarrow_array(suppressWarnings(sf::st_centroid(nc))),
  lines = geoarrow::as_geoarrow_array(sf::st_boundary(nc)),
  polygons = geoarrow::as_geoarrow_array(nc)
)

# the crs as parsed JSON, since Rust writes PROJJSON compactly and geoarrow R pretty prints it
crs_of <- function(x) {
  schema <- nanoarrow::infer_nanoarrow_schema(x)
  parsed <- geoarrow::geoarrow_schema_parse(schema)
  crs <- parsed$crs
  # a string crs, such as an authority code, is compared as it is
  list(
    crs = if (startsWith(crs, "{")) yyjsonr::read_json_str(crs) else crs,
    crs_type = parsed$crs_type
  )
}

is_geometry_array <- function(x) {
  if (!inherits(x, "nanoarrow_array")) {
    return(FALSE)
  }
  metadata <- nanoarrow::infer_nanoarrow_schema(x)$metadata
  name <- metadata[["ARROW:extension:name"]]
  !is.null(name) && startsWith(name, "geoarrow.")
}

# the first geometry array a function returns for one of the usual argument shapes, or NULL
geometry_result <- function(fn, g) {
  for (args in list(list(g), list(g, g), list(g, 1), list(g, 1, 1))) {
    out <- rlang::try_fetch(
      suppressWarnings(rlang::inject(fn(!!!args))),
      error = function(cnd) NULL
    )
    if (is_geometry_array(out)) {
      return(out)
    }
  }
  NULL
}

test_that("every function that returns geometries keeps the crs of its input", {
  withr::local_seed(1)
  ns <- asNamespace("geoarrowrs")
  fns <- grep("^ga_", getNamespaceExports("geoarrowrs"), value = TRUE)

  for (kind in names(inputs)) {
    g <- inputs[[kind]]
    want <- crs_of(g)
    checked <- 0L
    for (name in sort(fns)) {
      out <- geometry_result(get(name, envir = ns), g)
      if (is.null(out)) {
        next
      }
      checked <- checked + 1L
      expect_identical(
        crs_of(out),
        want,
        label = sprintf("the crs %s() returns for %s", name, kind)
      )
    }
    # a guard, so the test fails if the calls above stop reaching the functions
    expect_gt(checked, 15)
  }
})
