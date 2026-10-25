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

test_that("ga_point pairs two numeric vectors into points", {
  res <- ga_point(c(0, 1, 2), c(0, 1, 4))

  expect_setequal(as.character(sf::st_geometry_type(tosfc(res))), "POINT")
  expect_equal(coord_matrix(res), matrix(c(0, 1, 2, 0, 1, 4), ncol = 2))
})

test_that("ga_point is length preserving", {
  skip_deps()
  expect_equal(ga_point(runif(7), runif(7))$length, 7L)
})

test_that("ga_point recycles a length 1 coordinate", {
  skip_deps()
  expect_equal(
    coord_matrix(ga_point(c(0, 1, 2), 5)),
    matrix(c(0, 1, 2, 5, 5, 5), ncol = 2)
  )
  expect_equal(
    coord_matrix(ga_point(5, c(0, 1, 2))),
    matrix(c(5, 5, 5, 0, 1, 2), ncol = 2)
  )
})

test_that("ga_point rejects a length that cannot be recycled", {
  skip_deps()
  expect_error(ga_point(c(0, 1, 2), c(0, 1)), "`y` must be length 1")
  expect_error(ga_point(c(0, 1), c(0, 1, 2)), "`x` must be length 1")
})

test_that("ga_point gives a null point when either coordinate is NA", {
  skip_deps()
  res <- ga_point(c(0, NA, 2, NA), c(0, 1, NA, NA))

  expect_equal(res$length, 4L)
  expect_equal(res$null_count, 3L)
})

test_that("ga_point accepts arrow arrays as well as R vectors", {
  skip_deps()
  x <- nanoarrow::as_nanoarrow_array(c(0, 1, 2))
  y <- nanoarrow::as_nanoarrow_array(c(0, 1, 4))

  expect_equal(
    coord_matrix(ga_point(x, y)),
    coord_matrix(ga_point(c(0, 1, 2), c(0, 1, 4)))
  )
})

test_that("ga_point accepts integer vectors", {
  skip_deps()
  expect_equal(
    coord_matrix(ga_point(c(0L, 1L), c(2L, 3L))),
    matrix(c(0, 1, 2, 3), ncol = 2)
  )
})

test_that("ga_point records the crs it is given", {
  skip_deps()
  res <- ga_point(0, 0, crs = "EPSG:4326")

  expect_equal(wk::wk_crs(geoarrow::as_geoarrow_vctr(res)), "EPSG:4326")
})

test_that("ga_point without a crs has none", {
  skip_deps()
  expect_null(wk::wk_crs(geoarrow::as_geoarrow_vctr(ga_point(0, 0))))
})

test_that("ga_point output feeds the other functions", {
  skip_deps()
  pts <- ga_point(c(0, 3), c(0, 4))

  expect_equal(
    as.vector(ga_dist_euclidean_pairwise(pts, ga_point(c(0, 0), c(0, 0)))),
    c(0, 5)
  )
})

test_that("ga_point handles a zero length input", {
  skip_deps()
  expect_equal(ga_point(numeric(0), numeric(0))$length, 0L)
})

sf_envelope <- function(xmin, ymin, xmax, ymax) {
  boxes <- Map(
    function(a, b, c, d) {
      sf::st_as_sfc(sf::st_bbox(c(xmin = a, ymin = b, xmax = c, ymax = d)))[[1]]
    },
    xmin,
    ymin,
    xmax,
    ymax
  )
  geoarrow::as_geoarrow_array(sf::st_sfc(boxes))
}

test_that("ga_make_envelope matches sf's bbox polygons", {
  skip_deps()
  res <- ga_make_envelope(c(0, 10), c(0, -5), c(1, 12), c(2, 5))
  expected <- sf_envelope(c(0, 10), c(0, -5), c(1, 12), c(2, 5))

  expect_equal(as.vector(ga_equals_topo(res, expected)), c(TRUE, TRUE))
  expect_equal(as.vector(ga_unsigned_area(res)), c(2, 20))
  expect_equal(as.vector(ga_geometry_type(res)), c("POLYGON", "POLYGON"))
})

test_that("ga_make_envelope sorts swapped bounds", {
  skip_deps()
  expect_true(as.vector(ga_equals_topo(
    ga_make_envelope(1, 2, 0, 0),
    ga_make_envelope(0, 0, 1, 2)
  )))
})

test_that("ga_make_envelope recycles a length 1 bound", {
  skip_deps()
  res <- ga_make_envelope(0, 0, c(1, 2, 3), 1)
  expect_equal(as.vector(ga_unsigned_area(res)), c(1, 2, 3))
})

test_that("ga_make_envelope accepts integer vectors", {
  skip_deps()
  res <- ga_make_envelope(0L, 0L, 2L, 3L)
  expect_equal(as.vector(ga_unsigned_area(res)), 6)
})

test_that("ga_make_envelope gives a null polygon when any bound is NA", {
  skip_deps()
  res <- ga_make_envelope(c(0, NA), 0, 1, 1)

  expect_equal(res$length, 2L)
  expect_equal(as.vector(ga_geometry_type(res)), c("POLYGON", NA))
})

test_that("ga_make_envelope rejects a length that cannot be recycled", {
  skip_deps()
  expect_error(ga_make_envelope(c(0, 0), c(0, 0, 0), 1, 1), "xmin")
})

test_that("ga_make_envelope records the crs it is given", {
  skip_deps()
  res <- ga_make_envelope(0, 0, 1, 1, crs = "EPSG:4326")
  expect_equal(wk::wk_crs(geoarrow::as_geoarrow_vctr(res)), "EPSG:4326")
  expect_null(wk::wk_crs(geoarrow::as_geoarrow_vctr(ga_make_envelope(
    0,
    0,
    1,
    1
  ))))
})
