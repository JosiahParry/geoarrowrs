skip_if_no_sf <- function() {
  skip_if_not_installed("sf")
  skip_if_not_installed("nanoarrow")
}

nc_fgb <- function() {
  path <- tempfile(fileext = ".fgb")
  nc <- sf::st_read(system.file("shape/nc.shp", package = "sf"), quiet = TRUE)
  suppressWarnings(sf::st_write(nc, path, quiet = TRUE))
  path
}

test_that("properties and geometry round trip", {
  skip_if_no_sf()
  skip_if_not_installed("geoarrow")
  requireNamespace("geoarrow", quietly = TRUE)

  df <- as.data.frame(read_flatgeobuf(nc_fgb()))

  expect_equal(nrow(df), 100L)
  expect_true(all(c("NAME", "BIR74", "geometry") %in% names(df)))
  # flatgeobuf stores features in Hilbert R-tree order, so row order is not
  # the source order -- check membership rather than position
  expect_true("Ashe" %in% df$NAME)
  expect_setequal(
    as.character(sf::st_geometry_type(sf::st_as_sfc(df$geometry))),
    "MULTIPOLYGON"
  )
})

test_that("row order follows the spatial index, not the source order", {
  skip_if_no_sf()

  nc <- sf::st_read(system.file("shape/nc.shp", package = "sf"), quiet = TRUE)
  df <- as.data.frame(nanoarrow::convert_array_stream(read_flatgeobuf(nc_fgb())))

  expect_setequal(df$NAME, nc$NAME)
  expect_false(identical(df$NAME, nc$NAME))
})

test_that("bbox filters features spatially", {
  skip_if_no_sf()

  path <- nc_fgb()
  all_rows <- nrow(as.data.frame(nanoarrow::convert_array_stream(read_flatgeobuf(
    path
  ))))
  subset <- as.data.frame(
    nanoarrow::convert_array_stream(read_flatgeobuf(path, c(-79, 35, -78, 36)))
  )

  expect_equal(all_rows, 100L)
  expect_lt(nrow(subset), all_rows)
  expect_gt(nrow(subset), 0L)
})

test_that("a NULL bbox reads every feature", {
  skip_if_no_sf()

  path <- nc_fgb()
  df <- as.data.frame(nanoarrow::convert_array_stream(read_flatgeobuf(
    path,
    NULL
  )))
  expect_equal(nrow(df), 100L)
})

test_that("a bbox of the wrong length errors", {
  skip_if_no_sf()

  path <- nc_fgb()
  expect_error(read_flatgeobuf(path, c(-79, 35, -78)), "length 4")
  expect_error(read_flatgeobuf(path, c(1, 2, 3, 4, 5)), "length 4")
})

test_that("the geometry column carries a CRS", {
  skip_if_no_sf()

  schema <- nanoarrow::infer_nanoarrow_schema(read_flatgeobuf(nc_fgb()))
  meta <- schema$children$geometry$metadata[["ARROW:extension:metadata"]]

  expect_true(!is.null(meta))
  expect_match(meta, "crs", fixed = TRUE)
})

test_that("reading a missing file errors rather than crashing", {
  expect_error(read_flatgeobuf(tempfile(fileext = ".fgb")))
})
