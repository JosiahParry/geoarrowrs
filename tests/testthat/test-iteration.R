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

conv <- function(x) as.vector(nanoarrow::convert_array(x))
tosfc <- function(x) sf::st_as_sfc(geoarrow::as_geoarrow_vctr(x))

n_pts <- function(x) vapply(tosfc(x), nrow, integer(1))

outer_ring <- matrix(c(0, 0, 4, 0, 4, 4, 0, 4, 0, 0), ncol = 2, byrow = TRUE)
inner_ring <- matrix(c(1, 1, 2, 1, 2, 2, 1, 2, 1, 1), ncol = 2, byrow = TRUE)

square <- function() sf::st_polygon(list(outer_ring))
holed <- function() sf::st_polygon(list(outer_ring, inner_ring))

test_that("coords returns every vertex", {
  res <- tosfc(coords(ga(sf::st_sfc(square()))))

  expect_setequal(as.character(sf::st_geometry_type(res)), "MULTIPOINT")
  expect_equal(n_pts(coords(ga(sf::st_sfc(square())))), 5L)
})

test_that("coords includes interior rings", {
  expect_equal(n_pts(coords(ga(sf::st_sfc(holed())))), 10L)
})

test_that("exterior_coords skips interior rings", {
  expect_equal(n_pts(exterior_coords(ga(sf::st_sfc(holed())))), 5L)
})

test_that("coords is length preserving", {
  g <- ga(sf::st_sfc(square(), square(), square()))
  expect_length(tosfc(coords(g)), 3L)
})

test_that("n_coords agrees with coords", {
  g <- ga(sf::st_sfc(square(), holed()))

  expect_equal(conv(n_coords(g)), c(5L, 10L))
  expect_equal(conv(n_coords(g)), n_pts(coords(g)))
})

test_that("n_coords counts a point as one", {
  expect_equal(conv(n_coords(ga(sf::st_sfc(sf::st_point(c(0, 0)))))), 1L)
})

test_that("lines splits a ring into segments", {
  res <- tosfc(lines(ga(sf::st_sfc(square()))))

  expect_setequal(as.character(sf::st_geometry_type(res)), "MULTILINESTRING")
  expect_equal(lengths(res), 4L)
})

test_that("lines includes interior rings", {
  expect_equal(lengths(tosfc(lines(ga(sf::st_sfc(holed()))))), 8L)
})

test_that("every segment has two points", {
  res <- tosfc(lines(ga(sf::st_sfc(square()))))
  expect_true(all(vapply(res[[1]], nrow, integer(1)) == 2L))
})

test_that("lines is null for a point", {
  g <- ga(sf::st_sfc(square(), sf::st_point(c(0, 0))))
  res <- lines(g)

  expect_equal(res$length, 2L)
  expect_equal(res$null_count, 1L)
})

test_that("an empty geometry has no coordinates", {
  g <- ga(sf::st_sfc(square(), sf::st_polygon()))

  expect_equal(conv(n_coords(g)), c(5L, 0L))
  expect_equal(sf::st_is_empty(tosfc(coords(g))), c(FALSE, TRUE))
})

test_that("iteration reads a concrete array from a reader", {
  skip_deps()
  path <- system.file("shape/nc.shp", package = "sf")
  skip_if(path == "")

  g <- as.data.frame(read_shapefile(path))$geometry

  expect_length(conv(n_coords(g)), 100L)
  expect_length(tosfc(coords(g)), 100L)
  expect_true(all(conv(n_coords(g)) > 0))
})
