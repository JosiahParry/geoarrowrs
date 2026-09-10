skip_deps <- function() {
  skip_if_not_installed("sf")
  skip_if_not_installed("nanoarrow")
  skip_if_not_installed("geoarrow")
}

ga <- function(sfc) {
  skip_deps()
  requireNamespace("geoarrow", quietly = TRUE)
  geoarrow::as_geoarrow_array(sfc)
}

as_sfc <- function(res) sf::st_as_sfc(geoarrow::as_geoarrow_vctr(res))

test_that("to_radians converts degrees to radians", {
  g <- ga(sf::st_sfc(sf::st_point(c(180, 90))))
  coords <- sf::st_coordinates(as_sfc(ga_to_radians(g)))

  expect_equal(unname(coords[1, "X"]), pi, tolerance = 1e-12)
  expect_equal(unname(coords[1, "Y"]), pi / 2, tolerance = 1e-12)
})

test_that("to_degrees converts radians to degrees", {
  g <- ga(sf::st_sfc(sf::st_point(c(pi, pi / 2))))
  coords <- sf::st_coordinates(as_sfc(ga_to_degrees(g)))

  expect_equal(unname(coords[1, "X"]), 180, tolerance = 1e-12)
  expect_equal(unname(coords[1, "Y"]), 90, tolerance = 1e-12)
})

test_that("the two are inverses of one another", {
  original <- sf::st_sfc(sf::st_point(c(-79.5, 35.25)))
  g <- ga(original)
  round_tripped <- as_sfc(ga_to_degrees(ga_to_radians(g)))

  expect_equal(
    as.numeric(sf::st_coordinates(round_tripped)),
    as.numeric(sf::st_coordinates(original)),
    tolerance = 1e-12
  )
})

test_that("the output geometry type matches the input", {
  cases <- list(
    point = sf::st_sfc(sf::st_point(c(1, 2))),
    multipoint = sf::st_sfc(sf::st_multipoint(matrix(c(1, 2, 3, 4), ncol = 2))),
    linestring = sf::st_sfc(sf::st_linestring(matrix(
      c(0, 0, 1, 1),
      ncol = 2,
      byrow = TRUE
    ))),
    polygon = sf::st_sfc(sf::st_polygon(list(
      matrix(c(0, 0, 1, 0, 1, 1, 0, 0), ncol = 2, byrow = TRUE)
    )))
  )
  expected <- c("POINT", "MULTIPOINT", "LINESTRING", "POLYGON")

  for (i in seq_along(cases)) {
    out <- as_sfc(ga_to_radians(ga(cases[[i]])))
    expect_equal(as.character(sf::st_geometry_type(out)), expected[i])
  }
})

test_that("conversion is vectorized and preserves length", {
  g <- ga(sf::st_sfc(
    sf::st_point(c(180, 0)),
    sf::st_point(c(90, 0)),
    sf::st_point(c(0, 0))
  ))
  coords <- sf::st_coordinates(as_sfc(ga_to_radians(g)))

  expect_equal(nrow(coords), 3L)
  expect_equal(unname(coords[, "X"]), c(pi, pi / 2, 0), tolerance = 1e-12)
})

test_that("empty geometries stay empty", {
  g <- ga(sf::st_sfc(sf::st_point(c(1, 2)), sf::st_point()))
  out <- as_sfc(ga_to_radians(g))

  expect_length(out, 2L)
  expect_equal(sf::st_is_empty(out), c(FALSE, TRUE))
})

test_that("it works on a shapefile read from disk", {
  skip_deps()
  requireNamespace("geoarrow", quietly = TRUE)

  g <- as.data.frame(
    read_shapefile(system.file("shape/nc.shp", package = "sf"))
  )$geometry

  out <- as_sfc(ga_to_radians(g))
  expect_length(out, 100L)
  expect_setequal(as.character(sf::st_geometry_type(out)), "MULTIPOLYGON")
})
