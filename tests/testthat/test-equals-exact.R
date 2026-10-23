skip_deps <- function() {
  skip_if_not_installed("sf")
  skip_if_not_installed("geoarrow")
}

ga <- function(sfc) {
  skip_deps()
  geoarrow::as_geoarrow_array(sfc)
}

# sf compares every pair, so the row by row answer is the diagonal
sf_equals_exact <- function(x, y, tol) {
  diag(sf::st_equals_exact(x, y, par = tol, sparse = FALSE))
}

sq <- rbind(c(0, 0), c(1, 0), c(1, 1), c(0, 1), c(0, 0))
hole <- rbind(c(0.2, 0.2), c(0.2, 0.4), c(0.4, 0.4), c(0.4, 0.2), c(0.2, 0.2))

pairs <- function() {
  x <- sf::st_sfc(
    sf::st_point(c(0, 0)),
    sf::st_point(c(0, 0)),
    sf::st_point(c(0, 0)),
    sf::st_linestring(rbind(c(0, 0), c(1, 1))),
    sf::st_linestring(rbind(c(0, 0), c(1, 1))),
    sf::st_polygon(list(sq, hole)),
    sf::st_polygon(list(sq, hole)),
    sf::st_multipoint(rbind(c(0, 0), c(1, 1))),
    sf::st_multipolygon(list(list(sq), list(sq + 2))),
    sf::st_point(c(0, 0))
  )
  y <- sf::st_sfc(
    sf::st_point(c(0, 1e-7)),
    sf::st_point(c(1, 0)),
    sf::st_point(c(1, 1)),
    sf::st_linestring(rbind(c(1, 1), c(0, 0))),
    sf::st_linestring(rbind(c(0, 0), c(1, 1), c(2, 2))),
    sf::st_polygon(list(sq, hole + 1e-7)),
    sf::st_polygon(list(sq)),
    sf::st_multipoint(rbind(c(0, 0), c(1, 1 + 1e-7))),
    sf::st_multipolygon(list(list(sq), list(sq + 2))),
    sf::st_multipoint(rbind(c(0, 0)))
  )
  list(x = x, y = y)
}

test_that("equals_exact matches sf with no tolerance", {
  p <- pairs()
  expect_equal(
    as.vector(ga_equals_exact(ga(p$x), ga(p$y))),
    sf_equals_exact(p$x, p$y, 0)
  )
})

test_that("equals_exact matches sf with a tolerance", {
  p <- pairs()
  for (tol in c(1e-6, 1)) {
    expect_equal(
      as.vector(ga_equals_exact(ga(p$x), ga(p$y), tol)),
      sf_equals_exact(p$x, p$y, tol)
    )
  }
})

test_that("equals_exact allows a coordinate exactly at the tolerance", {
  x <- ga(sf::st_sfc(sf::st_point(c(0, 0))))
  y <- ga(sf::st_sfc(sf::st_point(c(1, 0))))
  expect_true(as.vector(ga_equals_exact(x, y, 1)))
})

test_that("equals_exact recycles y and tolerance", {
  x <- ga(sf::st_sfc(sf::st_point(c(0, 0)), sf::st_point(c(0, 0.5))))
  y <- ga(sf::st_sfc(sf::st_point(c(0, 0))))

  expect_equal(as.vector(ga_equals_exact(x, y)), c(TRUE, FALSE))
  expect_equal(as.vector(ga_equals_exact(x, y, c(0, 1))), c(TRUE, TRUE))
})

test_that("equals_exact rejects a tolerance of the wrong length", {
  x <- ga(sf::st_sfc(sf::st_point(c(0, 0)), sf::st_point(c(0, 0.5))))
  expect_error(ga_equals_exact(x, x, c(0, 1, 2)), "tolerance")
})

test_that("equals_exact is null propagating", {
  skip_deps()
  x <- geoarrow::as_geoarrow_array(wk::wkt(c("POINT (0 0)", NA)))
  y <- geoarrow::as_geoarrow_array(wk::wkt("POINT (0 0)"))

  res <- ga_equals_exact(x, y)
  expect_equal(res$length, 2L)
  expect_equal(as.vector(res), c(TRUE, NA))
})
