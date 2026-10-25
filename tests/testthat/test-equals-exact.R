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

# sf's sparse answer as plain integer vectors, one per row of x
sf_sparse_equals_exact <- function(x, y, tol) {
  lapply(sf::st_equals_exact(x, y, par = tol), as.integer)
}

ga_sparse <- function(x, y, tol) {
  lapply(as.vector(ga_sparse_equals_exact(ga(x), ga(y), tol)), as.integer)
}

test_that("the sparse form matches sf across every pair", {
  p <- pairs()
  for (tol in c(0, 1e-6, 1.5)) {
    expect_equal(
      ga_sparse(p$x, p$y, tol),
      sf_sparse_equals_exact(p$x, p$y, tol)
    )
  }
})

test_that("the sparse form matches sf on the nc counties and a shifted copy", {
  nc <- sf::st_geometry(sf::st_read(
    system.file("shape/nc.shp", package = "sf"),
    quiet = TRUE
  ))
  sf::st_crs(nc) <- NA
  shifted <- nc + c(0.001, 0)
  both <- c(nc, shifted)

  for (tol in c(0, 0.0005, 0.002)) {
    expect_equal(
      ga_sparse(nc, both, tol),
      sf_sparse_equals_exact(nc, both, tol)
    )
  }
})

test_that("the sparse form gives a null for a null x or an NA tolerance", {
  x <- ga(sf::st_sfc(sf::st_point(c(0, 0)), sf::st_point()))
  res <- ga_sparse_equals_exact(x, x, 0)
  expect_identical(res$null_count, 1L)

  res <- ga_sparse_equals_exact(x, x, c(NA, 0))
  expect_identical(res$null_count, 2L)
})

test_that("the masked st_equals_exact matches sf, sparse and dense", {
  p <- pairs()
  mask_sf()
  on.exit(detach("geoarrowrs:sf"), add = TRUE)
  x <- geoarrow::as_geoarrow_vctr(ga(p$x))
  y <- geoarrow::as_geoarrow_vctr(ga(p$y))

  expect_equal(
    lapply(as.vector(st_equals_exact(x, y, par = 1e-6)), as.integer),
    sf_sparse_equals_exact(p$x, p$y, 1e-6)
  )
  expect_identical(
    st_equals_exact(x, y, par = 1e-6, sparse = FALSE),
    sf::st_equals_exact(p$x, p$y, par = 1e-6, sparse = FALSE)
  )
})
