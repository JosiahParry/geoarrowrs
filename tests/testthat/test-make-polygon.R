skip_deps <- function() {
  skip_if_not_installed("sf")
  skip_if_not_installed("geoarrow")
}

as_wkt <- function(x) {
  sf::st_as_text(sf::st_as_sfc(geoarrow::as_geoarrow_vctr(x)))
}

as_lines <- function(wkt, ...) {
  geoarrow::as_geoarrow_array(
    wk::wkt(wkt),
    schema = geoarrow::geoarrow_linestring(...)
  )
}

as_multilines <- function(wkt) {
  geoarrow::as_geoarrow_array(
    wk::wkt(wkt),
    schema = geoarrow::geoarrow_multilinestring()
  )
}

ring <- function(x, y) cbind(c(x, x[1]), c(y, y[1]))

test_that("ga_make_polygon closes each shell, matching sf", {
  skip_deps()
  shell <- as_lines(c(
    "LINESTRING (0 0, 10 0, 10 10, 0 10)",
    "LINESTRING (20 0, 30 0, 20 10, 20 0)"
  ))
  want <- sf::st_as_text(sf::st_sfc(
    sf::st_polygon(list(ring(c(0, 10, 10, 0), c(0, 0, 10, 10)))),
    sf::st_polygon(list(ring(c(20, 30, 20), c(0, 0, 10))))
  ))

  expect_equal(as_wkt(ga_make_polygon(shell)), want)
})

test_that("holes on the same row become the holes of that polygon, matching sf", {
  skip_deps()
  shell <- as_lines(c(
    "LINESTRING (0 0, 10 0, 10 10, 0 10)",
    "LINESTRING (20 0, 30 0, 20 10)"
  ))
  holes <- as_multilines(c(
    "MULTILINESTRING ((1 1, 2 1, 2 2), (5 5, 6 5, 6 6, 5 5))",
    NA
  ))
  want <- sf::st_as_text(sf::st_sfc(
    sf::st_polygon(list(
      ring(c(0, 10, 10, 0), c(0, 0, 10, 10)),
      ring(c(1, 2, 2), c(1, 1, 2)),
      ring(c(5, 6, 6), c(5, 5, 6))
    )),
    sf::st_polygon(list(ring(c(20, 30, 20), c(0, 0, 10))))
  ))

  expect_equal(as_wkt(ga_make_polygon(shell, holes)), want)
})

test_that("close = FALSE builds each ring as given", {
  skip_deps()
  shell <- as_lines("LINESTRING (0 0, 10 0, 10 10, 0 10)")

  expect_equal(
    as_wkt(ga_make_polygon(shell, close = FALSE)),
    "POLYGON ((0 0, 10 0, 10 10, 0 10))"
  )
})

test_that("a null or short shell gives a null polygon and an empty one an empty polygon", {
  skip_deps()
  shell <- as_lines(c(
    "LINESTRING (0 0, 1 0, 1 1)",
    NA,
    "LINESTRING (0 0, 1 1)",
    "LINESTRING EMPTY"
  ))
  res <- ga_make_polygon(shell)

  expect_identical(res$length, 4L)
  expect_identical(res$null_count, 2L)
  expect_equal(
    as.vector(ga_geometry_type(res)),
    c("POLYGON", NA, NA, "POLYGON")
  )
  expect_equal(as.vector(ga_is_empty(res)), c(FALSE, NA, NA, TRUE))
})

test_that("a short hole gives a null polygon", {
  skip_deps()
  shell <- as_lines("LINESTRING (0 0, 10 0, 10 10, 0 10)")
  res <- ga_make_polygon(shell, as_multilines("MULTILINESTRING ((1 1, 2 2))"))

  expect_identical(res$null_count, 1L)
})

test_that("null and empty holes leave the shell alone", {
  skip_deps()
  shell <- as_lines(c(
    "LINESTRING (0 0, 10 0, 10 10, 0 10)",
    "LINESTRING (0 0, 10 0, 10 10, 0 10)"
  ))
  holes <- as_multilines(c(NA, "MULTILINESTRING EMPTY"))

  expect_equal(
    as_wkt(ga_make_polygon(shell, holes)),
    as_wkt(ga_make_polygon(shell))
  )
})

test_that("ga_make_polygon over ga_make_line_agg matches ga_polygon", {
  skip_deps()
  x <- c(0, 10, 10, 0, 20, 30, 20)
  y <- c(0, 0, 10, 10, 0, 0, 10)
  id <- c(1, 1, 1, 1, 2, 2, 2)

  expect_equal(
    as_wkt(ga_make_polygon(ga_make_line_agg(ga_xy(x, y), by = id))),
    as_wkt(ga_polygon(x, y, polygon_id = id))
  )
})

test_that("ga_make_polygon keeps z and crs", {
  skip_deps()
  shell <- as_lines("LINESTRING Z (0 0 1, 1 0 2, 1 1 3)", dimensions = "XYZ")
  expect_equal(
    as_wkt(ga_make_polygon(shell)),
    "POLYGON Z ((0 0 1, 1 0 2, 1 1 3, 0 0 1))"
  )

  res <- ga_make_polygon(ga_make_line_agg(
    ga_xy(c(0, 1, 1), c(0, 0, 1), crs = "EPSG:4326")
  ))
  expect_equal(
    sf::st_crs(sf::st_as_sfc(geoarrow::as_geoarrow_vctr(res))),
    sf::st_crs(4326)
  )
})

test_that("holes have to match shell in length and dimensions", {
  skip_deps()
  shell <- as_lines(c(
    "LINESTRING (0 0, 10 0, 10 10)",
    "LINESTRING (0 0, 10 0, 10 10)"
  ))
  hole <- as_multilines("MULTILINESTRING ((1 1, 2 1, 2 2))")

  expect_error(ga_make_polygon(shell, hole), "same length")
  expect_error(
    ga_make_polygon(
      as_lines("LINESTRING Z (0 0 1, 1 0 2, 1 1 3)", dimensions = "XYZ"),
      hole
    ),
    "same dimensions"
  )
})
