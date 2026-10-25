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

as_sfc <- function(x) sf::st_as_sfc(geoarrow::as_geoarrow_vctr(x))

# the GeoArrow extension metadata of x, parsed
metadata_of <- function(x) {
  schema <- nanoarrow::infer_nanoarrow_schema(x)
  yyjsonr::read_json_str(schema$metadata[["ARROW:extension:metadata"]])
}

# each function that takes a crs argument, called with that crs
with_crs <- list(
  ga_point = function(crs) ga_point(1, 2, crs = crs),
  ga_make_envelope = function(crs) ga_make_envelope(0, 0, 1, 1, crs = crs),
  ga_make_grid = function(crs) {
    ga_make_grid(cellsize = 1, offset = c(0, 0), n = 1, crs = crs)
  },
  ga_from_wkb = function(crs) {
    ga_from_wkb(ga_cast_geometry(ga_point(1, 2), "wkb"), crs = crs)
  }
)

test_that("a crs argument stores PROJJSON as a compact, typed JSON object", {
  projjson <- sf::st_crs(32119)$ProjJson
  for (name in names(with_crs)) {
    out <- with_crs[[name]](projjson)
    schema <- nanoarrow::infer_nanoarrow_schema(out)
    raw <- schema$metadata[["ARROW:extension:metadata"]]

    expect_identical(metadata_of(out)$crs_type, "projjson", label = name)
    expect_identical(
      metadata_of(out)$crs,
      yyjsonr::read_json_str(projjson),
      label = name
    )
    expect_false(grepl("\n", raw, fixed = TRUE), label = name)
    expect_true(sf::st_crs(as_sfc(out)) == sf::st_crs(32119), label = name)
  }
})

test_that("a crs argument stores AUTHORITY:CODE as an authority code", {
  for (name in names(with_crs)) {
    for (code in c("EPSG:4326", "OGC:CRS84", "IAU_2015:30100")) {
      meta <- metadata_of(with_crs[[name]](code))
      expect_identical(meta$crs, code, label = name)
      expect_identical(meta$crs_type, "authority_code", label = name)
    }
  }
})

test_that("a crs argument keeps any other string as given, untyped", {
  others <- c(
    sf::st_crs(4326)$wkt,
    "+proj=longlat +datum=WGS84",
    "urn:ogc:def:crs:EPSG::4326",
    '{"not": "projjson"}',
    # a valid PROJJSON document, but an ellipsoid rather than a CRS
    '{"type":"Ellipsoid","name":"e","semi_major_axis":1,"inverse_flattening":300}'
  )
  for (name in names(with_crs)) {
    for (crs in others) {
      meta <- metadata_of(with_crs[[name]](crs))
      expect_identical(meta$crs, crs, label = name)
      expect_null(meta$crs_type, label = name)
    }
  }
})

test_that("read_geojson() stores OGC:CRS84 as PROJJSON", {
  path <- withr::local_tempfile(fileext = ".geojson")
  writeLines(
    '{"type":"FeatureCollection","features":[{"type":"Feature","properties":{},"geometry":{"type":"Point","coordinates":[1,2]}}]}',
    path
  )
  geometry <- as.data.frame(read_geojson(path))$geometry

  expect_identical(metadata_of(geometry)$crs_type, "projjson")
  expect_true(sf::st_crs(as_sfc(geometry)) == sf::st_crs("OGC:CRS84"))
  expect_true(ga_is_longlat(geometry))
})
