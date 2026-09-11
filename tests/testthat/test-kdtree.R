skip_deps <- function() {
  skip_if_not_installed("sf")
  skip_if_not_installed("geoarrow")
}

nc_df <- function() {
  skip_deps()
  as.data.frame(read_shapefile(system.file("shape/nc.shp", package = "sf")))
}

centroids <- function() ga_centroid(nc_df()$geometry)

coords <- function(pts) {
  sf::st_coordinates(sf::st_as_sfc(geoarrow::as_geoarrow_vctr(pts)))
}

test_that("the tree indexes every finite point", {
  idx <- KDTree$new(centroids())

  expect_equal(idx$size(), 100L)
  expect_equal(idx$n_indexed(), 100L)
})

test_that("within agrees with a brute force radius test", {
  pts <- centroids()
  xy <- coords(pts)
  idx <- KDTree$new(pts)

  d <- sqrt((xy[, 1] + 78.6)^2 + (xy[, 2] - 35.8)^2)
  expect_equal(
    as.vector(idx$within(ga_xy(-78.6, 35.8), 0.5))[[1]],
    unname(which(d <= 0.5))
  )
})

test_that("range agrees with a brute force box test", {
  pts <- centroids()
  xy <- coords(pts)
  idx <- KDTree$new(pts)

  box <- ga_envelope(geoarrow::as_geoarrow_array(sf::st_sfc(
    sf::st_polygon(list(rbind(
      c(-79, 35),
      c(-78, 35),
      c(-78, 36),
      c(-79, 36),
      c(-79, 35)
    )))
  )))

  expect_equal(
    as.vector(idx$range(box))[[1]],
    unname(which(
      xy[, 1] >= -79 & xy[, 1] <= -78 & xy[, 2] >= 35 & xy[, 2] <= 36
    ))
  )
})

test_that("both queries are length preserving", {
  pts <- centroids()
  idx <- KDTree$new(pts)

  expect_equal(idx$within(pts, 0.5)$length, 100L)
  expect_equal(idx$range(nc_df()$geometry)$length, 100L)
})

test_that("every point is found within any radius of itself", {
  pts <- centroids()
  idx <- KDTree$new(pts)
  hits <- as.vector(idx$within(pts, 0.5))

  expect_true(all(mapply(function(h, i) i %in% h, hits, seq_along(hits))))
})

test_that("a county box holds its own centroid", {
  nc <- nc_df()
  idx <- KDTree$new(ga_centroid(nc$geometry))
  hits <- as.vector(idx$range(nc$geometry))

  expect_true(all(mapply(function(h, i) i %in% h, hits, seq_along(hits))))
})

test_that("the radius recycles", {
  pts <- centroids()
  idx <- KDTree$new(pts)

  expect_equal(
    as.vector(idx$within(pts, 0.5)),
    as.vector(idx$within(pts, rep(0.5, 100)))
  )
  expect_error(idx$within(pts, c(1, 2)), "length 1 or the same length")
})

test_that("a zero radius finds only a point sitting on the query", {
  idx <- KDTree$new(ga_xy(c(0, 1, 2), c(0, 1, 2)))

  expect_equal(as.vector(idx$within(ga_xy(1, 1), 0))[[1]], 2L)
  expect_length(as.vector(idx$within(ga_xy(9, 9), 0))[[1]], 0L)
})

test_that("a null point is left out of the tree and never returned", {
  idx <- KDTree$new(ga_xy(c(0, NA, 2), c(0, NA, 2)))

  expect_equal(idx$size(), 3L)
  expect_equal(idx$n_indexed(), 2L)
  expect_equal(as.vector(idx$within(ga_xy(1, 1), 5))[[1]], c(1L, 3L))
})

test_that("a null query point gives a null row", {
  idx <- KDTree$new(ga_xy(c(0, 2), c(0, 2)))
  res <- idx$within(ga_xy(c(0, NA), c(0, NA)), 1)

  expect_equal(res$length, 2L)
  expect_equal(res$null_count, 1L)
})

test_that("an array with no finite point cannot be indexed", {
  expect_error(KDTree$new(ga_xy(NA_real_, NA_real_)), "no finite points")
})

test_that("node_size must be at least 2", {
  expect_error(KDTree$new(ga_xy(0, 0), node_size = 1), "at least 2")
})
