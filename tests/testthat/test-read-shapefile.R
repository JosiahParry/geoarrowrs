skip_if_no_sf <- function() {
  skip_if_not_installed("sf")
  skip_if_not_installed("nanoarrow")
}

# Write an sfc to a shapefile in a fresh temp dir and read it back.
#
# GDAL's shapefile driver refuses to write a plain 3D geometry unless the
# concrete shape type is named: "Geometry type of `3D Polygon' not supported in
# shapefiles. Type can be overridden with a layer creation option of SHPT=...".
# Measured types do not need it.
roundtrip <- function(geometry, ..., name = "test", shpt = NULL) {
  dir <- file.path(tempdir(), paste0(name, "-", as.integer(runif(1, 0, 1e9))))
  dir.create(dir, recursive = TRUE, showWarnings = FALSE)
  path <- file.path(dir, paste0(name, ".shp"))

  df <- sf::st_sf(..., geometry = geometry)
  layer_options <- if (is.null(shpt)) character() else paste0("SHPT=", shpt)
  suppressWarnings(
    sf::st_write(
      df, path,
      quiet = TRUE, delete_dsn = TRUE, layer_options = layer_options
    )
  )
  read_shapefile(path)
}

# Pull the coordinate dimension out of the geometry column's schema by walking
# down to the coord node. Separated coords are a struct of x/y/z/m; interleaved
# coords are a fixed-size list whose child is named for the dimensions.
geom_dim <- function(stream) {
  schema <- nanoarrow::infer_nanoarrow_schema(stream)
  node <- schema$children$geometry
  repeat {
    nms <- names(node$children)
    if (is.null(nms) || length(nms) == 0) {
      return(NA_character_)
    }
    if (all(c("x", "y") %in% nms)) {
      return(toupper(paste0(intersect(c("x", "y", "z", "m"), nms), collapse = "")))
    }
    if (length(nms) == 1 && grepl("^[xyzm]+$", nms)) {
      return(toupper(nms))
    }
    node <- node$children[[1]]
  }
}

square <- function(dim = "XY") {
  coords <- switch(dim,
    XY   = matrix(c(0, 0,  1, 0,  1, 1,  0, 1,  0, 0), ncol = 2, byrow = TRUE),
    XYZ  = matrix(c(0, 0, 10,  1, 0, 11,  1, 1, 12,  0, 1, 13,  0, 0, 10),
                  ncol = 3, byrow = TRUE),
    XYM  = matrix(c(0, 0, 20,  1, 0, 21,  1, 1, 22,  0, 1, 23,  0, 0, 20),
                  ncol = 3, byrow = TRUE),
    XYZM = matrix(c(0, 0, 10, 20,  1, 0, 11, 21,  1, 1, 12, 22,
                    0, 1, 13, 23,  0, 0, 10, 20), ncol = 4, byrow = TRUE)
  )
  sf::st_sfc(sf::st_polygon(list(coords), dim = dim), crs = 4326)
}

test_that("XY polygons round trip with attributes", {
  skip_if_no_sf()

  stream <- roundtrip(
    square("XY"),
    id = 1L,
    label = "a",
    value = 1.5,
    name = "xy"
  )
  tbl <- as.data.frame(nanoarrow::convert_array_stream(stream))

  expect_equal(nrow(tbl), 1L)
  expect_true(all(c("id", "label", "value", "geometry") %in% names(tbl)))
  expect_equal(tbl$id, 1L)
  expect_equal(tbl$label, "a")
  expect_equal(tbl$value, 1.5)
})

test_that("geometry column carries the CRS from the .prj", {
  skip_if_no_sf()

  stream <- roundtrip(square("XY"), id = 1L, name = "crs")
  schema <- nanoarrow::infer_nanoarrow_schema(stream)
  meta <- schema$children$geometry$metadata[["ARROW:extension:metadata"]]

  expect_true(!is.null(meta))
  expect_match(meta, "crs", fixed = TRUE)
})

test_that("a Z shapefile with no measures is read as XYZ, not XYZM", {
  skip_if_no_sf()

  stream <- roundtrip(square("XYZ"), id = 1L, name = "z", shpt = "POLYGONZ")
  expect_equal(geom_dim(stream), "XYZ")
})

test_that("an M shapefile is read as XYM", {
  skip_if_no_sf()

  stream <- roundtrip(square("XYM"), id = 1L, name = "m")
  expect_equal(geom_dim(stream), "XYM")
})

test_that("a ZM shapefile is read as XYZM", {
  skip_if_no_sf()

  stream <- roundtrip(square("XYZM"), id = 1L, name = "zm")
  expect_equal(geom_dim(stream), "XYZM")
})

test_that("Z values survive the round trip", {
  skip_if_no_sf()
  skip_if_not_installed("arrow")
  # geoarrow registers the st_as_sf() method for arrow tables on load
  skip_if_not_installed("geoarrow")
  requireNamespace("geoarrow", quietly = TRUE)

  stream <- roundtrip(square("XYZ"), id = 1L, name = "zvals", shpt = "POLYGONZ")
  sfc <- sf::st_geometry(sf::st_as_sf(arrow::as_arrow_table(stream)))

  expect_equal(class(sfc[[1]])[1], "XYZ")
  zs <- sf::st_coordinates(sfc)[, "Z"]
  expect_setequal(unique(zs), c(10, 11, 12, 13))
})

test_that("point shapefiles round trip", {
  skip_if_no_sf()

  pts <- sf::st_sfc(
    sf::st_point(c(0, 1)),
    sf::st_point(c(2, 3)),
    crs = 4326
  )
  stream <- roundtrip(pts, id = 1:2, name = "pts")
  tbl <- as.data.frame(nanoarrow::convert_array_stream(stream))

  expect_equal(nrow(tbl), 2L)
  expect_equal(tbl$id, 1:2)
})

test_that("reading a missing file errors rather than crashing", {
  skip_if_no_sf()

  expect_error(read_shapefile(file.path(tempdir(), "does-not-exist.shp")))
})
