skip_deps <- function() {
  skip_if_not_installed("sf")
  skip_if_not_installed("geoarrow")
}

nc_geometry <- function() {
  skip_deps()
  path <- system.file("shape/nc.shp", package = "sf")
  skip_if(path == "")
  as.data.frame(read_shapefile(path))$geometry
}

sq <- function(xmin, ymin, side = 1) {
  sf::st_polygon(list(matrix(
    c(
      xmin,
      ymin,
      xmin + side,
      ymin,
      xmin + side,
      ymin + side,
      xmin,
      ymin + side,
      xmin,
      ymin
    ),
    ncol = 2,
    byrow = TRUE
  )))
}

boxes <- function() {
  skip_deps()
  geoarrow::as_geoarrow_array(sf::st_sfc(sq(0, 0), sq(2, 0), sq(4, 0)))
}

test_that("knn is length preserving and returns k rows each", {
  g <- boxes()
  got <- as.vector(ga_sparse_knn(g, g, k = 2))

  expect_length(got, 3L)
  expect_true(all(vapply(got, nrow, integer(1)) == 2L))
})

test_that("knn orders by distance and each box is nearest to itself", {
  g <- boxes()
  got <- as.vector(ga_sparse_knn(g, g, k = 3))

  expect_equal(as.integer(got[[1]]$row), c(1L, 2L, 3L))
  expect_equal(as.integer(got[[3]]$row), c(3L, 2L, 1L))
  expect_equal(got[[1]]$distance, c(0, 1, 3))
})

test_that("knn measures to the geometry, not to its bounding box", {
  skip_deps()
  # an L shape whose box corner is much nearer the point than the shape is
  el <- geoarrow::as_geoarrow_array(sf::st_sfc(sf::st_polygon(list(matrix(
    c(0, 0, 10, 0, 10, 1, 1, 1, 1, 10, 0, 10, 0, 0),
    ncol = 2,
    byrow = TRUE
  )))))
  pt <- ga_xy(9, 9)

  got <- as.vector(ga_sparse_knn(pt, el, k = 1))
  # the point sits inside the bounding box, so a box test would say 0
  expect_equal(got[[1]]$distance, 8)
  expect_equal(as.vector(ga_dist_euclidean_pairwise(ga_envelope(el), pt)), 0)
})

test_that("knn agrees with comparing every pair", {
  g <- nc_geometry()
  sites <- geoarrow::as_geoarrow_vctr(ga_xy(
    c(-78.6, -80.8, -77.0),
    c(35.8, 35.2, 34.9)
  ))
  got <- as.vector(ga_sparse_knn(sites, g, k = 5))

  for (i in seq_along(sites)) {
    d <- as.vector(ga_dist_euclidean_pairwise(g, sites[i]))
    want <- order(d, seq_along(d))[1:5]
    expect_equal(as.integer(got[[i]]$row), want)
    expect_equal(got[[i]]$distance, d[want])
  }
})

test_that("max_distance drops the rows beyond it", {
  g <- boxes()
  got <- as.vector(ga_sparse_knn(g, g, k = 3, max_distance = 1))

  expect_equal(as.integer(got[[1]]$row), c(1L, 2L))
  expect_equal(as.integer(got[[2]]$row), c(2L, 1L, 3L))
})

test_that("k larger than y returns every row", {
  g <- boxes()
  got <- as.vector(ga_sparse_knn(g, g, k = 10))
  expect_true(all(vapply(got, nrow, integer(1)) == 3L))
})

test_that("a null query geometry gives a null element", {
  skip_deps()
  g <- boxes()
  q <- geoarrow::as_geoarrow_array(sf::st_sfc(sq(0, 0), sf::st_polygon()))
  got <- ga_sparse_knn(q, g, k = 1)

  expect_equal(got$length, 2L)
  expect_equal(got$null_count, 1L)
})

test_that("a null row of y is never returned", {
  skip_deps()
  y <- geoarrow::as_geoarrow_array(sf::st_sfc(
    sq(0, 0),
    sf::st_polygon(),
    sq(2, 0)
  ))
  got <- as.vector(ga_sparse_knn(ga_xy(0.5, 0.5), y, k = 3))

  expect_equal(as.integer(got[[1]]$row), c(1L, 3L))
})

test_that("k is validated", {
  g <- boxes()
  expect_error(ga_sparse_knn(g, g, k = 0), "`k` must be at least 1")
})

test_that("max_distance is validated", {
  g <- boxes()
  expect_error(ga_sparse_knn(g, g, max_distance = -1), "must not be negative")
})

test_that("ga_knn_join attaches k rows per row of x with their distance", {
  g <- nc_geometry()
  counties <- data.frame(name = as.character(seq_along(g)), geometry = g)
  sites <- data.frame(
    site = c("a", "b"),
    geometry = geoarrow::as_geoarrow_vctr(ga_xy(c(-78.6, -80.8), c(35.8, 35.2)))
  )

  out <- as.data.frame(ga_knn_join(sites, counties, k = 3))

  expect_equal(nrow(out), 6L)
  expect_equal(out$site, rep(c("a", "b"), each = 3))
  expect_true(all(diff(out$distance[1:3]) >= 0))
  expect_equal(out$distance[c(1L, 4L)], c(0, 0))
})

test_that("ga_knn_join keeps unmatched rows only when left", {
  skip_deps()
  y <- data.frame(
    id = 1L,
    geometry = geoarrow::as_geoarrow_vctr(geoarrow::as_geoarrow_array(sf::st_sfc(
      sq(0, 0)
    )))
  )
  x <- data.frame(
    id = 1:2,
    geometry = geoarrow::as_geoarrow_vctr(ga_xy(c(0.5, 100), c(0.5, 100)))
  )

  left <- as.data.frame(ga_knn_join(x, y, max_distance = 1))
  inner <- as.data.frame(ga_knn_join(x, y, max_distance = 1, left = FALSE))

  expect_equal(nrow(left), 2L)
  expect_true(is.na(left$distance[2]))
  expect_true(is.na(left$id_y[2]))
  expect_equal(nrow(inner), 1L)
})

test_that("ga_knn_join suffixes shared names and can drop the distance", {
  skip_deps()
  frame <- function(id) {
    data.frame(
      id = id,
      geometry = geoarrow::as_geoarrow_vctr(ga_xy(id, id))
    )
  }

  out <- ga_knn_join(frame(1), frame(2), distance = NULL)

  expect_named(out, c("id_x", "geometry", "id_y"))
})
