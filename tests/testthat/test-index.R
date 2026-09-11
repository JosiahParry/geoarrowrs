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

conv <- function(x) as.vector(x)

#' The rows a single query box hits, now that the index takes an array
hits1 <- function(idx, xmin, ymin, xmax, ymax) {
  as.integer(as.vector(idx$search(ga(sf::st_sfc(box(
    xmin,
    ymin,
    xmax,
    ymax
  )))))[[1]])
}

#' The rows nearest a single point, now that the index takes an array
near1 <- function(idx, x, y, ...) {
  as.integer(as.vector(idx$neighbors(ga_xy(x, y), ...))[[1]])
}

box <- function(xmin, ymin, xmax, ymax) {
  sf::st_polygon(list(matrix(
    c(xmin, ymin, xmax, ymin, xmax, ymax, xmin, ymax, xmin, ymin),
    ncol = 2,
    byrow = TRUE
  )))
}

grid3 <- function() {
  sf::st_sfc(
    box(0, 0, 1, 1),
    box(2, 0, 3, 1),
    box(4, 0, 5, 1)
  )
}

test_that("an index reports its size", {
  idx <- RTree$new(ga(grid3()))

  expect_equal(idx$size(), 3L)
  expect_equal(idx$n_indexed(), 3L)
})

test_that("search finds the overlapping box", {
  idx <- RTree$new(ga(grid3()))

  expect_equal(hits1(idx, 0.5, 0.5, 0.6, 0.6), 1L)
  expect_equal(hits1(idx, 2.5, 0.5, 2.6, 0.6), 2L)
})

test_that("search returns rows in increasing order", {
  idx <- RTree$new(ga(grid3()))
  expect_equal(hits1(idx, -1, -1, 6, 2), c(1L, 2L, 3L))
})

test_that("search returns nothing when the box misses", {
  idx <- RTree$new(ga(grid3()))
  expect_length(hits1(idx, 90, 90, 91, 91), 0L)
})

test_that("a touching box still counts as a hit", {
  idx <- RTree$new(ga(grid3()))
  expect_true(1L %in% hits1(idx, 1, 0, 1.5, 1))
})

test_that("search is a superset of the exact predicate", {
  skip_deps()
  path <- system.file("shape/nc.shp", package = "sf")
  skip_if(path == "")

  g <- as.data.frame(read_shapefile(path))$geometry
  idx <- RTree$new(g)

  candidates <- hits1(idx, -79, 35, -78, 36)
  query <- ga(sf::st_sfc(box(-79, 35, -78, 36)))
  exact <- which(conv(ga_intersects(g, query)))

  expect_true(all(exact %in% candidates))
  expect_lte(length(candidates), 100L)
})

test_that("neighbors returns the requested count", {
  idx <- RTree$new(ga(grid3()))
  expect_length(near1(idx, 0, 0, k = 2), 2L)
})

test_that("neighbors orders by distance", {
  idx <- RTree$new(ga(grid3()))
  expect_equal(near1(idx, 0, 0, k = 3), c(1L, 2L, 3L))
  expect_equal(near1(idx, 5, 0, k = 3), c(3L, 2L, 1L))
})

test_that("neighbors honours max_distance", {
  idx <- RTree$new(ga(grid3()))
  near <- near1(idx, 0, 0, k = 3, max_distance = 1)

  expect_true(1L %in% near)
  expect_false(3L %in% near)
})

test_that("null geometries are left out of the tree", {
  g <- ga(sf::st_sfc(box(0, 0, 1, 1), sf::st_polygon(), box(2, 0, 3, 1)))
  idx <- RTree$new(g)

  expect_equal(idx$size(), 3L)
  expect_equal(idx$n_indexed(), 2L)
  expect_false(2L %in% hits1(idx, -1, -1, 9, 9))
})

test_that("row numbers survive a skipped geometry", {
  g <- ga(sf::st_sfc(sf::st_polygon(), box(2, 0, 3, 1)))
  idx <- RTree$new(g)

  expect_equal(hits1(idx, 2.5, 0.5, 2.6, 0.6), 2L)
})

test_that("node_size is validated", {
  expect_error(RTree$new(ga(grid3()), node_size = 1), "node_size")
})

test_that("k is validated", {
  idx <- RTree$new(ga(grid3()))
  expect_error(idx$neighbors(ga_xy(0, 0), k = 0), "`k` must be at least 1")
})

test_that("an array with no bounding box is rejected", {
  expect_error(
    RTree$new(ga(sf::st_sfc(sf::st_polygon(), sf::st_polygon()))),
    "bounding box"
  )
})

test_that("node_size does not change the answer", {
  g <- ga(grid3())
  small <- RTree$new(g, node_size = 2)
  large <- RTree$new(g, node_size = 64)

  expect_equal(
    hits1(small, -1, -1, 6, 2),
    hits1(large, -1, -1, 6, 2)
  )
})

test_that("ga_envelope returns a box array", {
  g <- ga(sf::st_sfc(box(0, 0, 1, 1), box(0, 0, 1, 1)))
  env <- ga_envelope(g)

  expect_equal(
    nanoarrow::infer_nanoarrow_schema(env)$metadata[["ARROW:extension:name"]],
    "geoarrow.box"
  )
  expect_equal(env$length, 2L)
})

test_that("ga_envelope passes a box array straight through", {
  g <- ga(sf::st_sfc(box(0, 0, 1, 1)))
  once <- ga_envelope(g)
  twice <- ga_envelope(once)

  expect_equal(
    as.character(geoarrow::as_geoarrow_vctr(twice)),
    as.character(geoarrow::as_geoarrow_vctr(once))
  )
})

test_that("ga_envelope is length preserving and keeps nulls null", {
  g <- ga(sf::st_sfc(box(0, 0, 1, 1), sf::st_polygon(), box(0, 0, 1, 1)))
  env <- ga_envelope(g)

  expect_equal(env$length, 3L)
  expect_equal(env$null_count, 1L)
})

test_that("the index can be packed with either sort", {
  g <- ga(sf::st_sfc(box(0, 0, 1, 1), box(5, 5, 6, 6), box(10, 10, 11, 11)))

  expect_equal(RTree$new(g)$size(), 3L)
  expect_equal(RTree$new(g, sort = "str")$size(), 3L)
  expect_equal(RTree$new(g, sort = "hilbert")$size(), 3L)
})

test_that("both sorts answer a query the same way", {
  g <- ga(sf::st_sfc(box(0, 0, 1, 1), box(5, 5, 6, 6), box(10, 10, 11, 11)))
  hilbert <- RTree$new(g, sort = "hilbert")$search(g)
  str <- RTree$new(g, sort = "str")$search(g)

  expect_equal(
    as.vector(hilbert),
    as.vector(str)
  )
})

test_that("an unknown sort is rejected", {
  g <- ga(sf::st_sfc(box(0, 0, 1, 1)))
  expect_error(RTree$new(g, sort = "quadtree"), "hilbert")
})

test_that("query is vectorised and length preserving", {
  g <- ga(sf::st_sfc(box(0, 0, 1, 1), box(5, 5, 6, 6), box(10, 10, 11, 11)))
  idx <- RTree$new(g)
  hits <- as.vector(idx$search(g))

  expect_length(hits, 3L)
  expect_true(all(vapply(
    seq_along(hits),
    function(i) i %in% hits[[i]],
    logical(1)
  )))
})

test_that("query accepts any geoarrow array", {
  g <- ga(sf::st_sfc(box(0, 0, 1, 1), box(5, 5, 6, 6)))
  idx <- RTree$new(g)

  from_polygons <- as.vector(idx$search(g))
  from_boxes <- as.vector(idx$search(ga_envelope(g)))
  from_points <- as.vector(idx$search(ga_centroid(g)))

  expect_equal(from_polygons, from_boxes)
  expect_length(from_points, 2L)
  expect_true(1 %in% from_points[[1]])
})

test_that("a null query geometry gives a null row", {
  g <- ga(sf::st_sfc(box(0, 0, 1, 1), box(5, 5, 6, 6)))
  idx <- RTree$new(g)
  res <- idx$search(ga(sf::st_sfc(box(0, 0, 1, 1), sf::st_polygon())))

  expect_equal(res$length, 2L)
  expect_equal(res$null_count, 1L)
})

test_that("query rows point back at the original array", {
  g <- ga(sf::st_sfc(box(0, 0, 1, 1), box(50, 50, 51, 51)))
  idx <- RTree$new(g)
  hits <- as.vector(idx$search(ga(sf::st_sfc(box(50, 50, 51, 51)))))

  expect_equal(hits[[1]], 2L)
})

test_that("neighbors is vectorised and length preserving", {
  g <- ga(grid3())
  idx <- RTree$new(g)
  res <- as.vector(idx$neighbors(ga_centroid(g), k = 2))

  expect_length(res, 3L)
  expect_true(all(lengths(res) == 2L))
  # each box is its own nearest neighbour
  expect_true(all(vapply(
    seq_along(res),
    function(i) res[[i]][1] == i,
    logical(1)
  )))
})

test_that("neighbors ranks by distance to the box", {
  skip_deps()
  path <- system.file("shape/nc.shp", package = "sf")
  skip_if(path == "")

  g <- as.data.frame(read_shapefile(path))$geometry
  boxes <- geoarrow::as_geoarrow_vctr(ga_envelope(g))
  cent <- geoarrow::as_geoarrow_vctr(ga_centroid(g))
  got <- as.vector(RTree$new(g)$neighbors(cent, k = 5))

  # ties are ordered arbitrarily, so compare the distances rather than the rows
  same <- vapply(
    seq_len(20),
    function(i) {
      d <- as.vector(ga_dist_euclidean_pairwise(boxes, cent[i]))
      isTRUE(all.equal(d[as.integer(got[[i]])], sort(d)[1:5]))
    },
    logical(1)
  )
  expect_true(all(same))
})

test_that("a null query row gives a null neighbour list", {
  g <- ga(grid3())
  idx <- RTree$new(g)
  res <- idx$neighbors(ga_xy(c(0, NA), c(0, NA)), k = 1)

  expect_equal(res$length, 2L)
  expect_equal(res$null_count, 1L)
})
