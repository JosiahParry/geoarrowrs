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

two_blobs <- function() {
  skip_deps()
  ga_xy(c(0, 0.1, 0.2, 5, 5.1, 5.2), c(0, 0.1, 0.2, 5, 5.1, 5.2))
}

blob_and_stray <- function() {
  skip_deps()
  ga_xy(c(0, 0.1, 0.2, 0.3, 9), c(0, 0.1, 0.2, 0.3, 9))
}

test_that("dbscan separates two blobs", {
  res <- as.vector(ga_dbscan(two_blobs(), eps = 1, min_points = 2))

  expect_length(res, 6L)
  expect_equal(length(unique(res)), 2L)
  expect_equal(res[1], res[2])
  expect_false(res[1] == res[4])
})

test_that("dbscan marks a stray point as noise", {
  res <- as.vector(ga_dbscan(blob_and_stray(), eps = 1, min_points = 2))

  expect_length(res, 5L)
  expect_true(is.na(res[5]))
  expect_false(anyNA(res[1:4]))
})

test_that("dbscan labels start at one", {
  res <- as.vector(ga_dbscan(two_blobs(), eps = 1, min_points = 2))
  expect_equal(min(res, na.rm = TRUE), 1L)
})

test_that("kmeans splits into exactly k clusters", {
  res <- as.vector(ga_kmeans(two_blobs(), k = 2, seed = 1))

  expect_length(res, 6L)
  expect_equal(sort(unique(res)), c(1L, 2L))
  expect_false(anyNA(res))
})

test_that("kmeans is reproducible with a seed", {
  a <- as.vector(ga_kmeans(two_blobs(), k = 2, seed = 42))
  b <- as.vector(ga_kmeans(two_blobs(), k = 2, seed = 42))

  expect_equal(a, b)
})

test_that("outlier_scores gives one score per row", {
  res <- as.vector(ga_outlier_scores(blob_and_stray(), k_neighbours = 2))

  expect_length(res, 5L)
  expect_type(res, "double")
  expect_gt(res[5], res[1])
})

test_that("clustering is length preserving", {
  g <- two_blobs()

  expect_equal(ga_dbscan(g, eps = 1, min_points = 2)$length, g$length)
  expect_equal(ga_kmeans(g, k = 2, seed = 1)$length, g$length)
  expect_equal(ga_outlier_scores(g, k_neighbours = 2)$length, g$length)
})

test_that("a row that is not a single point comes back null", {
  g <- ga(sf::st_sfc(
    sf::st_point(c(0, 0)),
    sf::st_point(),
    sf::st_point(c(5, 5))
  ))
  res <- ga_dbscan(g, eps = 1, min_points = 1)

  expect_equal(res$length, 3L)
  expect_equal(res$null_count, 1L)
})

test_that("a multipoint array has no single points to cluster", {
  g <- ga(sf::st_sfc(
    sf::st_multipoint(cbind(c(0, 0.1), c(0, 0.1))),
    sf::st_multipoint(cbind(c(1, 2), c(1, 2)))
  ))
  res <- ga_dbscan(g, eps = 1, min_points = 1)

  expect_equal(res$length, 2L)
  expect_equal(res$null_count, 2L)
})

test_that("explode turns a multipoint into rows that can be clustered", {
  mp <- ga(sf::st_sfc(sf::st_multipoint(cbind(
    c(0, 0.1, 0.2, 5, 5.1, 5.2),
    c(0, 0.1, 0.2, 5, 5.1, 5.2)
  ))))
  pts <- ga_cast_geometry(ga_flatten(ga_explode(mp)), "point")
  res <- as.vector(ga_dbscan(pts, eps = 1, min_points = 2))

  expect_length(res, 6L)
  expect_equal(length(unique(res)), 2L)
})

test_that("whole array parameters must be a single value", {
  g <- two_blobs()

  expect_error(ga_dbscan(g, eps = c(1, 2), min_points = 2), "single value")
  expect_error(ga_dbscan(g, eps = 1, min_points = c(1, 2)), "single value")
  expect_error(ga_kmeans(g, k = c(1, 2)), "single value")
  expect_error(ga_outlier_scores(g, k_neighbours = c(1, 2)), "single value")
})

test_that("out of range parameters are rejected", {
  g <- two_blobs()

  expect_error(ga_dbscan(g, eps = 1, min_points = 0), "at least 1")
  expect_error(ga_kmeans(g, k = 0), "at least 1")
  expect_error(ga_outlier_scores(g, k_neighbours = 0), "at least 1")
})
