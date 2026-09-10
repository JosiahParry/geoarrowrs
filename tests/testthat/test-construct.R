skip_deps <- function() {
  skip_if_not_installed("sf")
  skip_if_not_installed("nanoarrow")
  skip_if_not_installed("geoarrow")
}

tosfc <- function(x) {
  skip_deps()
  requireNamespace("geoarrow", quietly = TRUE)
  sf::st_as_sfc(geoarrow::as_geoarrow_vctr(x))
}

coord_matrix <- function(x) {
  unname(sf::st_coordinates(tosfc(x))[, 1:2, drop = FALSE])
}

test_that("ga_xy pairs two numeric vectors into points", {
  res <- ga_xy(c(0, 1, 2), c(0, 1, 4))

  expect_setequal(as.character(sf::st_geometry_type(tosfc(res))), "POINT")
  expect_equal(coord_matrix(res), matrix(c(0, 1, 2, 0, 1, 4), ncol = 2))
})

test_that("ga_xy is length preserving", {
  skip_deps()
  expect_equal(ga_xy(runif(7), runif(7))$length, 7L)
})

test_that("ga_xy recycles a length 1 coordinate", {
  skip_deps()
  expect_equal(
    coord_matrix(ga_xy(c(0, 1, 2), 5)),
    matrix(c(0, 1, 2, 5, 5, 5), ncol = 2)
  )
  expect_equal(
    coord_matrix(ga_xy(5, c(0, 1, 2))),
    matrix(c(5, 5, 5, 0, 1, 2), ncol = 2)
  )
})

test_that("ga_xy rejects a length that cannot be recycled", {
  skip_deps()
  expect_error(ga_xy(c(0, 1, 2), c(0, 1)), "`y` must be length 1")
  expect_error(ga_xy(c(0, 1), c(0, 1, 2)), "`x` must be length 1")
})

test_that("ga_xy gives a null point when either coordinate is NA", {
  skip_deps()
  res <- ga_xy(c(0, NA, 2, NA), c(0, 1, NA, NA))

  expect_equal(res$length, 4L)
  expect_equal(res$null_count, 3L)
})

test_that("ga_xy accepts arrow arrays as well as R vectors", {
  skip_deps()
  x <- nanoarrow::as_nanoarrow_array(c(0, 1, 2))
  y <- nanoarrow::as_nanoarrow_array(c(0, 1, 4))

  expect_equal(
    coord_matrix(ga_xy(x, y)),
    coord_matrix(ga_xy(c(0, 1, 2), c(0, 1, 4)))
  )
})

test_that("ga_xy accepts integer vectors", {
  skip_deps()
  expect_equal(
    coord_matrix(ga_xy(c(0L, 1L), c(2L, 3L))),
    matrix(c(0, 1, 2, 3), ncol = 2)
  )
})

test_that("ga_xy records the crs it is given", {
  skip_deps()
  res <- ga_xy(0, 0, crs = "EPSG:4326")

  expect_equal(wk::wk_crs(geoarrow::as_geoarrow_vctr(res)), "EPSG:4326")
})

test_that("ga_xy without a crs has none", {
  skip_deps()
  expect_null(wk::wk_crs(geoarrow::as_geoarrow_vctr(ga_xy(0, 0))))
})

test_that("ga_xy output feeds the other functions", {
  skip_deps()
  pts <- ga_xy(c(0, 3), c(0, 4))

  expect_equal(
    as.vector(ga_dist_euclidean_pairwise(pts, ga_xy(c(0, 0), c(0, 0)))),
    c(0, 5)
  )
})

test_that("ga_xy handles a zero length input", {
  skip_deps()
  expect_equal(ga_xy(numeric(0), numeric(0))$length, 0L)
})
