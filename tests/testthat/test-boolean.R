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

tosfc <- function(x) sf::st_as_sfc(geoarrow::as_geoarrow_vctr(x))
area <- function(x) as.numeric(sf::st_area(tosfc(x)))

box <- function(xmin, ymin, xmax, ymax) {
  sf::st_polygon(list(matrix(
    c(xmin, ymin, xmax, ymin, xmax, ymax, xmin, ymax, xmin, ymin),
    ncol = 2,
    byrow = TRUE
  )))
}

a <- function() box(0, 0, 2, 2)
b <- function() box(1, 1, 3, 3)

test_that("boolean_intersection keeps the shared area", {
  x <- ga(sf::st_sfc(a()))
  y <- ga(sf::st_sfc(b()))

  expect_equal(area(boolean_intersection(x, y)), 1)
})

test_that("boolean_union keeps the combined area", {
  x <- ga(sf::st_sfc(a()))
  y <- ga(sf::st_sfc(b()))

  expect_equal(area(boolean_union(x, y)), 7)
})

test_that("boolean_difference removes the overlap", {
  x <- ga(sf::st_sfc(a()))
  y <- ga(sf::st_sfc(b()))

  expect_equal(area(boolean_difference(x, y)), 3)
})

test_that("boolean_xor keeps what is in exactly one", {
  x <- ga(sf::st_sfc(a()))
  y <- ga(sf::st_sfc(b()))

  expect_equal(area(boolean_xor(x, y)), 6)
})

test_that("the four operations agree with sf", {
  sa <- sf::st_sfc(a())
  sb <- sf::st_sfc(b())
  x <- ga(sa)
  y <- ga(sb)

  expect_equal(
    area(boolean_intersection(x, y)),
    as.numeric(sf::st_area(sf::st_intersection(sa, sb)))
  )
  expect_equal(
    area(boolean_union(x, y)),
    as.numeric(sf::st_area(sf::st_union(sa, sb)))
  )
  expect_equal(
    area(boolean_difference(x, y)),
    as.numeric(sf::st_area(sf::st_difference(sa, sb)))
  )
})

test_that("disjoint inputs give an empty intersection", {
  x <- ga(sf::st_sfc(a()))
  y <- ga(sf::st_sfc(box(9, 9, 10, 10)))

  expect_equal(area(boolean_intersection(x, y)), 0)
})

test_that("results are always multipolygons", {
  x <- ga(sf::st_sfc(a()))
  y <- ga(sf::st_sfc(b()))
  res <- tosfc(boolean_union(x, y))

  expect_setequal(as.character(sf::st_geometry_type(res)), "MULTIPOLYGON")
})

test_that("boolean ops are length preserving", {
  x <- ga(sf::st_sfc(a(), a(), a()))
  y <- ga(sf::st_sfc(b()))

  expect_length(tosfc(boolean_intersection(x, y)), 3L)
  expect_equal(area(boolean_intersection(x, y)), rep(1, 3))
})

test_that("boolean ops reject a mismatched length", {
  x <- ga(sf::st_sfc(a(), a()))
  y <- ga(sf::st_sfc(b(), b(), b()))

  expect_error(boolean_intersection(x, y), "y")
})

test_that("a non polygonal row comes back null", {
  x <- ga(sf::st_sfc(a(), sf::st_point(c(0, 0))))
  y <- ga(sf::st_sfc(b()))
  res <- boolean_intersection(x, y)

  expect_equal(res$length, 2L)
  expect_equal(res$null_count, 1L)
  expect_true(sf::st_is_empty(tosfc(res))[2])
})

test_that("unary_union dissolves the whole array to length one", {
  x <- ga(sf::st_sfc(a(), b()))
  res <- unary_union(x)

  expect_length(tosfc(res), 1L)
  expect_equal(area(res), 7)
})

test_that("unary_union matches folding boolean_union", {
  x <- ga(sf::st_sfc(a(), b()))
  pair <- boolean_union(ga(sf::st_sfc(a())), ga(sf::st_sfc(b())))

  expect_equal(area(unary_union(x)), area(pair))
})

test_that("unary_union keeps disjoint parts separate", {
  x <- ga(sf::st_sfc(a(), box(9, 9, 10, 10)))
  res <- unary_union(x)

  expect_length(tosfc(res), 1L)
  expect_equal(lengths(tosfc(res)), 2L)
  expect_equal(area(res), 5)
})

test_that("unary_union skips non polygonal rows", {
  x <- ga(sf::st_sfc(a(), sf::st_point(c(0, 0))))
  expect_equal(area(unary_union(x)), 4)
})

test_that("boolean ops read a concrete array from a reader", {
  skip_deps()
  path <- system.file("shape/nc.shp", package = "sf")
  skip_if(path == "")

  g <- as.data.frame(read_shapefile(path))$geometry
  dissolved <- unary_union(g)

  expect_length(tosfc(dissolved), 1L)
  expect_gt(area(dissolved), 0)
})
