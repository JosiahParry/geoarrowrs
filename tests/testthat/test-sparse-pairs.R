test_that("ga_sparse_pairs expands matches to one row per pair", {
  x <- ga_xy(c(0, 5, 1), c(0, 5, 1))
  y <- ga_xy(c(0, 1), c(0, 1))
  hits <- ga_sparse_intersects(x, y)

  pairs <- as.data.frame(arrow::as_arrow_array(ga_sparse_pairs(hits)))
  expect_identical(pairs$x, c(1L, 3L))
  expect_identical(pairs$y, c(1L, 2L))
})

test_that("left keeps the rows that matched nothing, with a null y", {
  x <- ga_xy(c(0, 5, 1), c(0, 5, 1))
  y <- ga_xy(c(0, 1), c(0, 1))
  hits <- ga_sparse_intersects(x, y)

  pairs <- as.data.frame(
    arrow::as_arrow_array(ga_sparse_pairs(hits, left = TRUE))
  )
  expect_identical(pairs$x, c(1L, 2L, 3L))
  expect_identical(pairs$y, c(1L, NA, 2L))
})

test_that("ga_sparse_pairs agrees with Arrow's own flatten on the inner case", {
  skip_if_not_installed("sf")
  skip_if_not_installed("geoarrow")

  nc <- as.data.frame(read_shapefile(
    system.file("shape/nc.shp", package = "sf")
  ))
  hits <- ga_sparse_touches(nc$geometry, nc$geometry)
  pairs <- as.data.frame(arrow::as_arrow_array(ga_sparse_pairs(hits)))

  parents <- arrow::call_function(
    "list_parent_indices",
    arrow::as_arrow_array(hits)
  )
  flat <- arrow::call_function("list_flatten", arrow::as_arrow_array(hits))
  expect_identical(pairs$x, as.vector(parents) + 1L)
  expect_identical(pairs$y, as.integer(as.vector(flat)))
})

test_that("a row of x with no geometry gives a null y under left", {
  x <- geoarrow::as_geoarrow_array(wk::wkt(c("POINT (0 0)", NA)))
  y <- ga_xy(0, 0)
  hits <- ga_sparse_intersects(x, y)

  pairs <- as.data.frame(
    arrow::as_arrow_array(ga_sparse_pairs(hits, left = TRUE))
  )
  expect_identical(pairs$x, c(1L, 2L))
  expect_identical(pairs$y, c(1L, NA))

  pairs <- as.data.frame(arrow::as_arrow_array(ga_sparse_pairs(hits)))
  expect_identical(pairs$x, 1L)
})

test_that("ga_sparse_pairs rejects something that is not a sparse result", {
  expect_error(ga_sparse_pairs(arrow::Array$create(1:3)), "list array")
})
