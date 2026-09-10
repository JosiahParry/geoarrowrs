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

square <- function() {
  sf::st_polygon(list(matrix(
    c(0, 0, 1, 0, 1, 1, 0, 1, 0, 0),
    ncol = 2,
    byrow = TRUE
  )))
}

identity_args <- list(a = 1, b = 0, xoff = 0, d = 0, e = 1, yoff = 0)

test_that("the identity transform changes nothing", {
  g <- ga(sf::st_sfc(square()))
  res <- affine_transform(g, 1, 0, 0, 0, 1, 0)

  expect_equal(
    sf::st_coordinates(tosfc(res))[, 1:2],
    sf::st_coordinates(tosfc(g))[, 1:2]
  )
})

test_that("it reproduces translate", {
  g <- ga(sf::st_sfc(square()))

  expect_equal(
    sf::st_coordinates(tosfc(affine_transform(g, 1, 0, 5, 0, 1, 3)))[, 1:2],
    sf::st_coordinates(tosfc(translate(g, 5, 3)))[, 1:2]
  )
})

test_that("it reproduces scale_xy about the origin", {
  line <- sf::st_linestring(cbind(c(0, 2), c(0, 4)))
  g <- ga(sf::st_sfc(line))
  scaled <- sf::st_coordinates(tosfc(affine_transform(g, 2, 0, 0, 0, 3, 0)))

  expect_equal(scaled[, 1], c(0, 4))
  expect_equal(scaled[, 2], c(0, 12))
})

test_that("area scales by the determinant", {
  g <- ga(sf::st_sfc(square()))
  res <- affine_transform(g, 2, 0, 0, 0, 3, 0)

  expect_equal(as.numeric(sf::st_area(tosfc(res))), 6)
})

test_that("the geometry type is preserved", {
  poly <- tosfc(affine_transform(ga(sf::st_sfc(square())), 1, 0, 0, 0, 1, 0))
  expect_setequal(as.character(sf::st_geometry_type(poly)), "POLYGON")

  pt <- tosfc(affine_transform(
    ga(sf::st_sfc(sf::st_point(c(1, 1)))),
    1,
    0,
    0,
    0,
    1,
    0
  ))
  expect_setequal(as.character(sf::st_geometry_type(pt)), "POINT")
})

test_that("coefficients recycle per row", {
  g <- ga(sf::st_sfc(square(), square()))
  res <- affine_transform(g, 1, 0, c(0, 10), 0, 1, 0)
  xs <- sf::st_coordinates(tosfc(res))[, 1]

  expect_equal(max(xs[1:5]), 1)
  expect_equal(max(xs[6:10]), 11)
})

test_that("a wrong length is rejected", {
  g <- ga(sf::st_sfc(square(), square()))
  expect_error(affine_transform(g, 1, 0, c(1, 2, 3), 0, 1, 0), "xoff")
})

test_that("length is preserved", {
  g <- ga(sf::st_sfc(square(), square(), square()))
  expect_length(tosfc(affine_transform(g, 1, 0, 1, 0, 1, 1)), 3L)
})

test_that("an unsupported geometry type errors", {
  g <- ga(sf::st_sfc(sf::st_geometrycollection(list(sf::st_point(c(0, 0))))))
  expect_error(affine_transform(g, 1, 0, 0, 0, 1, 0), "Expected")
})
