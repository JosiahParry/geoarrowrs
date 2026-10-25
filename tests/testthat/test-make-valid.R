skip_if_not_installed("sf")
skip_if_not_installed("geoarrow")

poly <- function(...) {
  sf::st_polygon(lapply(list(...), function(m) {
    matrix(m, ncol = 2, byrow = TRUE)
  }))
}
square <- function(x0, y0, x1, y1) c(x0, y0, x1, y0, x1, y1, x0, y1, x0, y0)

as_sfc <- function(x) sf::st_as_sfc(geoarrow::as_geoarrow_vctr(x))

same_shape <- function(a, b) {
  mapply(function(x, y) sf::st_equals(x, y, sparse = FALSE)[1, 1], a, b)
}

test_that("ga_make_valid matches sf where no rings overlap", {
  g <- sf::st_sfc(
    poly(square(0, 0, 10, 10)),
    # a bowtie crossing at (5, 5)
    poly(c(0, 0, 10, 10, 10, 0, 0, 10, 0, 0)),
    # a spike out to (15, 5) and back
    poly(c(0, 0, 10, 0, 15, 5, 10, 0, 10, 10, 0, 10, 0, 0)),
    # a hole sharing an edge with its shell
    poly(square(0, 0, 10, 10), c(5, 2, 5, 7, 10, 7, 10, 2, 5, 2)),
    # a hole outside its shell
    poly(square(0, 0, 10, 10), square(20, 20, 30, 30)),
    # a ring that touches itself at (5, 0)
    poly(c(0, 0, 10, 0, 10, 10, 5, 0, 0, 10, 0, 0))
  )

  got <- as_sfc(ga_make_valid(geoarrow::as_geoarrow_array(g)))
  want <- sf::st_make_valid(g)

  expect_true(all(sf::st_is_valid(got)))
  expect_true(all(same_shape(got, want)))
  expect_equal(as.numeric(sf::st_area(got)), as.numeric(sf::st_area(want)))
})

test_that("overlapping rings follow the odd-even rule rather than sf", {
  g <- sf::st_sfc(
    # two holes overlapping in a 2 by 2 square
    poly(square(0, 0, 10, 10), square(1, 1, 6, 6), square(4, 4, 9, 9)),
    # a hole inside a hole
    poly(square(0, 0, 10, 10), square(1, 1, 9, 9), square(3, 3, 7, 7)),
    # two parts overlapping in a 5 by 5 square
    sf::st_multipolygon(list(
      list(matrix(square(0, 0, 10, 10), ncol = 2, byrow = TRUE)),
      list(matrix(square(5, 5, 15, 15), ncol = 2, byrow = TRUE))
    ))
  )

  res <- ga_make_valid(geoarrow::as_geoarrow_array(g))

  expect_true(all(as.vector(ga_is_valid(res))))
  expect_equal(as.vector(ga_unsigned_area(res)), c(58, 52, 150))
})

test_that("ga_make_valid keeps the shape of valid polygons", {
  nc <- as.data.frame(read_shapefile(system.file(
    "shape/nc.shp",
    package = "sf"
  )))
  res <- ga_make_valid(nc$geometry)

  expect_equal(unique(as.vector(ga_geometry_type(res))), "MULTIPOLYGON")
  expect_true(all(same_shape(as_sfc(res), as_sfc(nc$geometry))))
})

test_that("a null, a geometry that is not polygonal, or a NaN coordinate gives a null", {
  g <- geoarrow::as_geoarrow_array(wk::wkt(c(
    "POLYGON ((0 0, 1 0, 1 1, 0 0))",
    NA,
    "POINT (1 1)",
    "LINESTRING (0 0, 1 1)"
  )))
  res <- ga_make_valid(g)

  expect_identical(res$null_count, 3L)
  expect_equal(as.vector(ga_geometry_type(res)), c("MULTIPOLYGON", NA, NA, NA))

  # ga_polygon() reads NaN as a coordinate, not as NA
  expect_identical(
    ga_make_valid(ga_polygon(c(0, NaN, 1), c(0, 0, 1)))$null_count,
    1L
  )
})

test_that("an empty polygon gives an empty multipolygon", {
  res <- ga_make_valid(geoarrow::as_geoarrow_array(wk::wkt("POLYGON EMPTY")))

  expect_identical(res$null_count, 0L)
  expect_true(as.vector(ga_is_empty(res)))
})

test_that("ga_make_valid repairs a box array and keeps crs", {
  boxes <- ga_envelope(ga_polygon(c(0, 2, 2), c(0, 0, 3)))
  expect_equal(as.vector(ga_unsigned_area(ga_make_valid(boxes))), 6)

  res <- ga_make_valid(ga_polygon(c(0, 1, 1), c(0, 0, 1), crs = "EPSG:4326"))
  expect_equal(sf::st_crs(as_sfc(res)), sf::st_crs(4326))
})

test_that("sf's and sdf's st_make_valid dispatch to ga_make_valid", {
  bowtie <- sf::st_sfc(poly(c(0, 0, 10, 10, 10, 0, 0, 10, 0, 0)))
  v <- geoarrow::as_geoarrow_vctr(geoarrow::as_geoarrow_array(bowtie))

  fixed <- sf::st_make_valid(v)
  expect_s3_class(fixed, "geoarrow_vctr")
  expect_true(as.vector(ga_is_valid(fixed)))

  skip_if_not_installed("sdf")
  expect_s3_class(sdf::st_make_valid(v), "geoarrow_vctr")
})
