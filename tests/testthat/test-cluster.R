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

lst <- function(x) nanoarrow::convert_array(x)

two_blobs <- function() {
  sf::st_multipoint(cbind(
    c(0, 0.1, 0.2, 5, 5.1, 5.2),
    c(0, 0.1, 0.2, 5, 5.1, 5.2)
  ))
}

blob_and_stray <- function() {
  sf::st_multipoint(cbind(
    c(0, 0.1, 0.2, 0.3, 9),
    c(0, 0.1, 0.2, 0.3, 9)
  ))
}

test_that("dbscan separates two blobs", {
  res <- lst(ga_dbscan(ga(sf::st_sfc(two_blobs())), eps = 1, min_points = 2))

  expect_length(res, 1L)
  expect_length(res[[1]], 6L)
  expect_equal(length(unique(res[[1]])), 2L)
  expect_equal(res[[1]][1], res[[1]][2])
  expect_false(res[[1]][1] == res[[1]][4])
})

test_that("dbscan marks a stray point as noise", {
  res <- lst(ga_dbscan(
    ga(sf::st_sfc(blob_and_stray())),
    eps = 1,
    min_points = 3
  ))

  expect_true(is.na(res[[1]][5]))
  expect_false(anyNA(res[[1]][1:4]))
})

test_that("a larger eps merges the blobs", {
  merged <- lst(ga_dbscan(
    ga(sf::st_sfc(two_blobs())),
    eps = 10,
    min_points = 2
  ))
  expect_equal(length(unique(merged[[1]])), 1L)
})

test_that("dbscan is length preserving", {
  g <- ga(sf::st_sfc(two_blobs(), two_blobs(), two_blobs()))
  expect_length(lst(ga_dbscan(g, eps = 1, min_points = 2)), 3L)
})

test_that("dbscan recycles and validates its arguments", {
  g <- ga(sf::st_sfc(two_blobs(), two_blobs()))

  expect_length(lst(ga_dbscan(g, eps = c(1, 10), min_points = 2)), 2L)
  expect_error(ga_dbscan(g, eps = c(1, 2, 3), min_points = 2), "eps")
})

test_that("kmeans produces exactly k clusters", {
  res <- lst(ga_kmeans(ga(sf::st_sfc(two_blobs())), k = 2, seed = 1))

  expect_length(res[[1]], 6L)
  expect_equal(length(unique(res[[1]])), 2L)
  expect_false(anyNA(res[[1]]))
})

test_that("kmeans with a seed is reproducible", {
  g <- ga(sf::st_sfc(two_blobs()))

  expect_equal(
    lst(ga_kmeans(g, k = 2, seed = 42))[[1]],
    lst(ga_kmeans(g, k = 2, seed = 42))[[1]]
  )
})

test_that("kmeans groups the blobs together", {
  res <- lst(ga_kmeans(ga(sf::st_sfc(two_blobs())), k = 2, seed = 1))[[1]]

  expect_equal(length(unique(res[1:3])), 1L)
  expect_equal(length(unique(res[4:6])), 1L)
  expect_false(res[1] == res[4])
})

test_that("kmeans is length preserving", {
  g <- ga(sf::st_sfc(two_blobs(), two_blobs()))
  expect_length(lst(ga_kmeans(g, k = 2, seed = 1)), 2L)
})

test_that("outlier_scores flags the stray point", {
  res <- lst(ga_outlier_scores(
    ga(sf::st_sfc(blob_and_stray())),
    k_neighbours = 2
  ))

  expect_length(res[[1]], 5L)
  expect_equal(which.max(res[[1]]), 5L)
  expect_gt(res[[1]][5], res[[1]][1])
})

test_that("outlier_scores is length preserving", {
  g <- ga(sf::st_sfc(blob_and_stray(), blob_and_stray()))
  expect_length(lst(ga_outlier_scores(g, k_neighbours = 2)), 2L)
})

test_that("a non point geometry comes back null", {
  square <- sf::st_polygon(list(matrix(
    c(0, 0, 1, 0, 1, 1, 0, 1, 0, 0),
    ncol = 2,
    byrow = TRUE
  )))
  g <- ga(sf::st_sfc(two_blobs(), square))
  res <- ga_dbscan(g, eps = 1, min_points = 2)

  expect_equal(res$length, 2L)
  expect_equal(res$null_count, 1L)
})
