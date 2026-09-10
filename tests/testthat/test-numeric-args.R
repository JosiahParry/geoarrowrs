skip_deps <- function() {
  skip_if_not_installed("sf")
  skip_if_not_installed("nanoarrow")
  skip_if_not_installed("geoarrow")
}

squares <- function(n = 2) {
  skip_deps()
  requireNamespace("geoarrow", quietly = TRUE)
  polys <- lapply(seq_len(n), function(i) {
    sf::st_polygon(list(matrix(
      c(i, i, i + 1, i, i + 1, i + 1, i, i + 1, i, i),
      ncol = 2,
      byrow = TRUE
    )))
  })
  geoarrow::as_geoarrow_array(sf::st_sfc(polys))
}

test_that("numeric args accept a plain double", {
  g <- squares()
  expect_no_error(ga_simplify(g, 0.01))
})

test_that("numeric args accept an integer", {
  g <- squares()
  expect_no_error(ga_simplify(g, 1L))
})

test_that("numeric args accept an arrow array", {
  g <- squares()
  expect_no_error(ga_simplify(g, nanoarrow::as_nanoarrow_array(0.01)))
})

test_that("a length-1 value recycles and a full-length vector is elementwise", {
  g <- squares(3)
  expect_no_error(ga_simplify(g, 0.01))
  expect_no_error(ga_simplify(g, c(0.01, 0.02, 0.03)))
})

test_that("a wrong-length vector errors", {
  g <- squares(3)
  expect_error(ga_simplify(g, c(0.01, 0.02)), "length 1 or the same length")
})

test_that("NA is carried through as null rather than erroring", {
  g <- squares()
  expect_no_error(ga_simplify(g, NA_real_))
})

test_that("a non-numeric arg errors clearly", {
  g <- squares()
  expect_error(ga_simplify(g, "nope"), "numeric vector or a float 64 array")
})

test_that("geometry args accept concrete arrays, not just mixed ones", {
  skip_deps()
  requireNamespace("geoarrow", quietly = TRUE)

  # read_shapefile emits a multipolygon array; these all used to demand a
  # mixed geoarrow.geometry array and error on it
  g <- as.data.frame(
    read_shapefile(system.file("shape/nc.shp", package = "sf"))
  )$geometry

  expect_no_error(ga_centroid(g))
  expect_no_error(ga_signed_area(g))
  expect_no_error(ga_unsigned_area(g))
  expect_no_error(ga_bounding_rect(g))
  expect_no_error(ga_convex_hull(g))
  expect_no_error(ga_minimum_rotated_rect(g))
  expect_no_error(ga_extremes(g))
  expect_no_error(ga_perimeter_unsigned_geodesic(g))
  expect_no_error(ga_buffer(g, 0.1, "round", "round", 5, 8))
})
