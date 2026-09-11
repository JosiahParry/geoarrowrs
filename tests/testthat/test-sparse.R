skip_if_not_installed("sf")
skip_if_not_installed("geoarrow")

nc_path <- system.file("shape/nc.shp", package = "sf")
nc <- as.data.frame(read_shapefile(nc_path))

as_lists <- function(x) lapply(as.vector(x), as.integer)

test_that("sparse predicates match sf", {
  nc_sf <- sf::st_read(nc_path, quiet = TRUE)
  g <- nc$geometry

  expect_identical(
    as_lists(ga_sparse_intersects(g, g)),
    lapply(sf::st_intersects(nc_sf), as.integer)
  )
  expect_identical(
    as_lists(ga_sparse_touches(g, g)),
    lapply(sf::st_touches(nc_sf), as.integer)
  )
  expect_identical(
    as_lists(ga_sparse_overlaps(g, g)),
    # sf warns that it treats longitude and latitude as planar, which is the
    # comparison being made here
    lapply(suppressWarnings(sf::st_overlaps(nc_sf)), as.integer)
  )
})

test_that("sparse predicates are length preserving", {
  res <- ga_sparse_intersects(nc$geometry, nc$geometry)
  expect_identical(res$length, length(nc$geometry))
})

test_that("within and contains are transposes", {
  pts <- geoarrow::as_geoarrow_vctr(ga_centroid(nc$geometry))
  counties <- nc$geometry

  within <- as_lists(ga_sparse_within(pts, counties))
  contains <- as_lists(ga_sparse_contains(counties, pts))

  pairs_within <- do.call(rbind, Map(cbind, seq_along(within), within))
  pairs_contains <- do.call(rbind, Map(cbind, contains, seq_along(contains)))
  expect_identical(pairs_within, pairs_contains)
})

test_that("a row that matches nothing is empty, not null", {
  far <- geoarrow::as_geoarrow_vctr(ga_xy(0, 0))
  res <- as.vector(ga_sparse_intersects(far, nc$geometry))
  expect_length(res, 1L)
  expect_length(res[[1L]], 0L)
})

test_that("a null query row gives a null element", {
  g <- geoarrow::as_geoarrow_vctr(ga_xy(c(-78.6, NA), c(35.8, NA)))
  res <- as.vector(ga_sparse_within(g, nc$geometry))
  expect_length(res[[1L]], 1L)
  expect_null(res[[2L]])
})

test_that("a target with no boxes matches nothing", {
  empty <- geoarrow::as_geoarrow_vctr(ga_xy(NA_real_, NA_real_))
  res <- as.vector(ga_sparse_intersects(nc$geometry, empty))
  expect_length(res, length(nc$geometry))
  expect_true(all(lengths(res) == 0L))
})

test_that("x and y need not be the same length", {
  pts <- ga_xy(c(-78.6, -80.8, 0), c(35.8, 35.2, 0))
  res <- ga_sparse_within(pts, nc$geometry)
  expect_identical(res$length, 3L)
})
