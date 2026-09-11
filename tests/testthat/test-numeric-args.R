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
  expect_no_error(ga_perimeter_geodesic(g))
  expect_no_error(ga_buffer(g, 0.1))
})

test_that("ga_buffer defaults to a round cap and join", {
  sq <- geoarrow::as_geoarrow_array(sf::st_sfc(
    sf::st_polygon(list(rbind(c(0, 0), c(2, 0), c(2, 2), c(0, 2), c(0, 0))))
  ))

  # the sides add 8, the four corners add a unit circle
  expect_equal(
    as.vector(ga_unsigned_area(ga_buffer(sq, 1))),
    12 + pi,
    tolerance = 0.01
  )
  expect_equal(
    as.vector(ga_unsigned_area(ga_buffer(sq, 1, line_join = "bevel"))),
    14
  )
})

test_that("round_angle is an angle, so a smaller step is smoother", {
  sq <- geoarrow::as_geoarrow_array(sf::st_sfc(
    sf::st_polygon(list(rbind(c(0, 0), c(2, 0), c(2, 2), c(0, 2), c(0, 0))))
  ))

  coarse <- as.vector(ga_unsigned_area(ga_buffer(sq, 1, round_angle = 0.5)))
  fine <- as.vector(ga_unsigned_area(ga_buffer(sq, 1, round_angle = 0.01)))

  expect_lt(coarse, fine)
  expect_lt(fine, 12 + pi)
})

test_that("a point buffers to a disc", {
  res <- ga_buffer(ga_xy(0, 0), 1)

  expect_equal(res$length, 1L)
  expect_equal(as.vector(ga_unsigned_area(res)), pi, tolerance = 0.01)
})
