skip_deps <- function() {
  skip_if_not_installed("sf")
  skip_if_not_installed("geoarrow")
}

ga <- function(sfc) {
  skip_deps()
  geoarrow::as_geoarrow_array(sfc)
}

sq <- rbind(c(0, 0), c(1, 0), c(1, 1), c(0, 1), c(0, 0))

every_type <- function() {
  sf::st_sfc(
    sf::st_point(c(0, 0)),
    sf::st_linestring(rbind(c(0, 0), c(1, 1))),
    sf::st_polygon(list(sq)),
    sf::st_multipoint(rbind(c(0, 0), c(1, 1))),
    sf::st_multilinestring(list(rbind(c(0, 0), c(1, 1)))),
    sf::st_multipolygon(list(list(sq))),
    sf::st_geometrycollection(list(sf::st_point(c(0, 0))))
  )
}

test_that("geometry_type matches sf for every simple features type", {
  x <- every_type()
  expect_equal(
    as.vector(ga_geometry_type(ga(x))),
    as.character(sf::st_geometry_type(x))
  )
})

test_that("geometry_type reads a single typed array", {
  x <- sf::st_sfc(sf::st_polygon(list(sq)), sf::st_polygon(list(sq * 2)))
  expect_equal(
    as.vector(ga_geometry_type(ga(x))),
    as.character(sf::st_geometry_type(x))
  )
})

test_that("geometry_type is length preserving and null propagating", {
  skip_deps()
  g <- geoarrow::as_geoarrow_array(
    wk::wkt(c("POINT (0 0)", NA, "LINESTRING (0 0, 1 1)"))
  )
  res <- ga_geometry_type(g)

  expect_equal(res$length, 3L)
  expect_equal(as.vector(res), c("POINT", NA, "LINESTRING"))
})

test_that("ga_is matches sf for one type", {
  x <- every_type()
  for (type in as.character(sf::st_geometry_type(x))) {
    expect_equal(as.vector(ga_is(ga(x), type)), sf::st_is(x, type))
  }
})

test_that("ga_is matches sf for several types", {
  x <- every_type()
  types <- c("POINT", "MULTIPOLYGON", "GEOMETRYCOLLECTION")
  expect_equal(as.vector(ga_is(ga(x), types)), sf::st_is(x, types))
})

test_that("ga_is is null propagating", {
  skip_deps()
  g <- geoarrow::as_geoarrow_array(wk::wkt(c("POINT (0 0)", NA)))
  expect_equal(as.vector(ga_is(g, "POINT")), c(TRUE, NA))
})

test_that("ga_is rejects an unknown type name", {
  g <- ga(sf::st_sfc(sf::st_point(c(0, 0))))
  expect_error(ga_is(g, "POLYGN"), "POLYGN")
  expect_error(ga_is(g, 1), "character")
})
