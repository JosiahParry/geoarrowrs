skip_deps <- function() {
  skip_if_not_installed("sf")
  skip_if_not_installed("geoarrow")
}

ga <- function(sfc) {
  skip_deps()
  geoarrow::as_geoarrow_array(sfc)
}

as_wkt <- function(x) {
  sf::st_as_text(sf::st_as_sfc(geoarrow::as_geoarrow_vctr(x)))
}

extension_name <- function(x) {
  nanoarrow::infer_nanoarrow_schema(x)$metadata[["ARROW:extension:name"]]
}

test_that("translate matches sf on a multipoint array", {
  x <- sf::st_sfc(sf::st_multipoint(rbind(c(0, 0), c(1, 1))))
  g <- ga(x)
  res <- ga_translate(g, 2, -3)

  expect_identical(extension_name(res), extension_name(g))
  expect_equal(as_wkt(res), sf::st_as_text(x + c(2, -3)))
})

test_that("translate matches sf on a mixed array with a collection", {
  x <- sf::st_sfc(
    sf::st_point(c(0, 0)),
    sf::st_linestring(rbind(c(0, 0), c(1, 1))),
    sf::st_geometrycollection(list(
      sf::st_point(c(5, 5)),
      sf::st_polygon(list(rbind(c(0, 0), c(1, 0), c(1, 1), c(0, 0))))
    ))
  )
  g <- ga(x)
  res <- ga_translate(g, 10, 20)

  expect_identical(extension_name(res), extension_name(g))
  expect_equal(as_wkt(res), sf::st_as_text(x + c(10, 20)))
})

test_that("translate recycles per row offsets on a mixed array", {
  x <- sf::st_sfc(
    sf::st_point(c(0, 0)),
    sf::st_linestring(rbind(c(1, 1), c(2, 2)))
  )
  res <- ga_translate(ga(x), c(1, 2), c(10, 20))

  expected <- x +
    list(sf::st_point(c(1, 10)), sf::st_point(c(2, 20)))
  expect_equal(as_wkt(res), sf::st_as_text(expected))
})

test_that("translate is null propagating on any geometry type", {
  skip_deps()
  g <- geoarrow::as_geoarrow_array(
    wk::wkt(c("MULTIPOINT ((0 0))", NA, "POINT (1 1)"))
  )
  res <- ga_translate(g, c(1, 1, NA), 1)

  expect_equal(res$length, 3L)
  expect_equal(
    as.vector(ga_geometry_type(res)),
    c("MULTIPOINT", NA, NA)
  )
})

test_that("translate still keeps a concrete polygon array", {
  x <- sf::st_sfc(sf::st_polygon(list(
    rbind(c(0, 0), c(1, 0), c(1, 1), c(0, 0))
  )))
  g <- ga(x)
  res <- ga_translate(g, 1, 1)

  expect_identical(extension_name(res), extension_name(g))
  expect_equal(as_wkt(res), sf::st_as_text(x + c(1, 1)))
})
