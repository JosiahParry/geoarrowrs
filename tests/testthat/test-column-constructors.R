skip_deps <- function() {
  skip_if_not_installed("sf")
  skip_if_not_installed("geoarrow")
}

as_wkt <- function(x) {
  sf::st_as_text(sf::st_as_sfc(geoarrow::as_geoarrow_vctr(x)))
}

sf_wkt <- function(...) sf::st_as_text(sf::st_sfc(...))

ring <- function(x, y) cbind(c(x, x[1]), c(y, y[1]))

test_that("ga_linestring makes one line per linestring_id, matching sf", {
  skip_deps()
  x <- c(0, 1, 2, 10, 11)
  y <- c(0, 1, 0, 5, 5)

  expect_equal(
    as_wkt(ga_linestring(x, y, linestring_id = c(1, 1, 1, 2, 2))),
    sf_wkt(
      sf::st_linestring(cbind(x[1:3], y[1:3])),
      sf::st_linestring(cbind(x[4:5], y[4:5]))
    )
  )
  # without linestring_id, every row is one line
  expect_equal(
    as_wkt(ga_linestring(x, y)),
    sf_wkt(sf::st_linestring(cbind(x, y)))
  )
})

test_that("ga_linestring keeps z and m, matching sf", {
  skip_deps()
  expect_equal(
    as_wkt(ga_linestring(c(0, 1), c(0, 1), z = c(5, 6))),
    sf_wkt(sf::st_linestring(cbind(c(0, 1), c(0, 1), c(5, 6))))
  )
  expect_equal(
    as_wkt(ga_linestring(c(0, 1), c(0, 1), m = c(5, 6))),
    sf_wkt(sf::st_linestring(cbind(c(0, 1), c(0, 1), c(5, 6)), dim = "XYM"))
  )
})

test_that("ga_multipoint makes one multipoint per multipoint_id, matching sf", {
  skip_deps()
  x <- c(0, 1, 5)
  y <- c(0, 1, 5)

  expect_equal(
    as_wkt(ga_multipoint(x, y, multipoint_id = c("b", "b", "a"))),
    sf_wkt(
      sf::st_multipoint(cbind(x[1:2], y[1:2])),
      sf::st_multipoint(cbind(x[3], y[3]))
    )
  )
})

test_that("ga_multilinestring nests lines inside each multilinestring, matching sf", {
  skip_deps()
  x <- c(0, 1, 5, 6, 10, 11, 20, 21)
  y <- c(0, 1, 5, 6, 0, 1, 0, 1)
  # the line ids restart at 1 in each multilinestring
  outer <- c(1, 1, 1, 1, 2, 2, 2, 2)
  inner <- c(1, 1, 2, 2, 1, 1, 2, 2)

  expect_equal(
    as_wkt(ga_multilinestring(
      x,
      y,
      multilinestring_id = outer,
      linestring_id = inner
    )),
    sf_wkt(
      sf::st_multilinestring(list(
        cbind(x[1:2], y[1:2]),
        cbind(x[3:4], y[3:4])
      )),
      sf::st_multilinestring(list(
        cbind(x[5:6], y[5:6]),
        cbind(x[7:8], y[7:8])
      ))
    )
  )
})

test_that("ga_multipolygon nests polygons and holes, matching sf", {
  skip_deps()
  # a square with a hole and a second square, then a triangle
  x <- c(0, 10, 10, 0, 2, 4, 4, 2, 20, 21, 21, 20, 50, 60, 50)
  y <- c(0, 0, 10, 10, 2, 2, 4, 4, 0, 0, 1, 1, 0, 0, 10)
  multi <- c(rep(1, 12), 2, 2, 2)
  polygon <- c(rep(1, 8), rep(2, 4), 1, 1, 1)
  linestring <- c(rep(1, 4), rep(2, 4), rep(1, 4), 1, 1, 1)

  expect_equal(
    as_wkt(ga_multipolygon(
      x,
      y,
      multipolygon_id = multi,
      polygon_id = polygon,
      linestring_id = linestring
    )),
    sf_wkt(
      sf::st_multipolygon(list(
        list(ring(x[1:4], y[1:4]), ring(x[5:8], y[5:8])),
        list(ring(x[9:12], y[9:12]))
      )),
      sf::st_multipolygon(list(list(ring(x[13:15], y[13:15]))))
    )
  )
})

test_that("ga_multipolygon keeps z", {
  skip_deps()
  expect_equal(
    as_wkt(ga_multipolygon(c(0, 1, 1), c(0, 0, 1), z = c(5, 6, 7))),
    "MULTIPOLYGON Z (((0 0 5, 1 0 6, 1 1 7, 0 0 5)))"
  )
})

test_that("ga_multipolygon rejects a ring too short to close", {
  skip_deps()
  expect_error(
    ga_multipolygon(c(0, 1), c(0, 1)),
    "ring starting at row 1 has 3 coordinates"
  )
})

test_that("an outermost id that returns is an error", {
  skip_deps()
  x <- c(0, 1, 2, 3)
  again <- c(1, 2, 1, 1)

  expect_error(ga_linestring(x, x, linestring_id = again), "row 3 returns")
  expect_error(ga_multipoint(x, x, multipoint_id = again), "row 3 returns")
  expect_error(
    ga_multilinestring(x, x, multilinestring_id = again),
    "row 3 returns"
  )
})

test_that("the column constructors take Arrow arrays", {
  skip_deps()
  x <- c(0, 1, 2, 10, 11)
  y <- c(0, 1, 0, 5, 5)
  id <- c(1L, 1L, 1L, 2L, 2L)

  expect_equal(
    as_wkt(ga_linestring(
      nanoarrow::as_nanoarrow_array(x),
      nanoarrow::as_nanoarrow_array(y),
      linestring_id = nanoarrow::as_nanoarrow_array(id)
    )),
    as_wkt(ga_linestring(x, y, linestring_id = id))
  )
})

test_that("the column constructors reject NA and mismatched lengths", {
  skip_deps()
  expect_error(ga_linestring(c(0, NA), c(0, 1)), "`x` must not contain NA")
  expect_error(ga_multipoint(c(0, 1), c(0, 1, 2)), "same length")
  expect_error(
    ga_multilinestring(c(0, 1), c(0, 1), linestring_id = c(1, NA)),
    "`linestring_id` must not contain NA"
  )
})

test_that("the column constructors record crs", {
  skip_deps()
  res <- ga_multipoint(c(0, 1), c(0, 1), crs = "EPSG:4326")

  expect_equal(
    sf::st_crs(sf::st_as_sfc(geoarrow::as_geoarrow_vctr(res))),
    sf::st_crs(4326)
  )
})
