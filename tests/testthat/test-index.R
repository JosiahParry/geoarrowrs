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

  expect_equal(conv(idx$search(0.5, 0.5, 0.6, 0.6)), 1L)
  expect_equal(conv(idx$search(2.5, 0.5, 2.6, 0.6)), 2L)
})

test_that("search returns rows in increasing order", {
  idx <- RTree$new(ga(grid3()))
  expect_equal(conv(idx$search(-1, -1, 6, 2)), c(1L, 2L, 3L))
})

test_that("search returns nothing when the box misses", {
  idx <- RTree$new(ga(grid3()))
  expect_length(conv(idx$search(90, 90, 91, 91)), 0L)
})

test_that("a touching box still counts as a hit", {
  idx <- RTree$new(ga(grid3()))
  expect_true(1L %in% conv(idx$search(1, 0, 1.5, 1)))
})

test_that("search is a superset of the exact predicate", {
  skip_deps()
  path <- system.file("shape/nc.shp", package = "sf")
  skip_if(path == "")

  g <- as.data.frame(read_shapefile(path))$geometry
  idx <- RTree$new(g)

  candidates <- conv(idx$search(-79, 35, -78, 36))
  query <- ga(sf::st_sfc(box(-79, 35, -78, 36)))
  exact <- which(conv(ga_intersects(g, query)))

  expect_true(all(exact %in% candidates))
  expect_lte(length(candidates), 100L)
})

test_that("neighbors returns the requested count", {
  idx <- RTree$new(ga(grid3()))
  expect_length(conv(idx$neighbors(0, 0, max_results = 2)), 2L)
})

test_that("neighbors orders by distance", {
  idx <- RTree$new(ga(grid3()))
  expect_equal(conv(idx$neighbors(0, 0, max_results = 3)), c(1L, 2L, 3L))
  expect_equal(conv(idx$neighbors(5, 0, max_results = 3)), c(3L, 2L, 1L))
})

test_that("neighbors honours max_distance", {
  idx <- RTree$new(ga(grid3()))
  near <- conv(idx$neighbors(0, 0, max_results = 3, max_distance = 1))

  expect_true(1L %in% near)
  expect_false(3L %in% near)
})

test_that("null geometries are left out of the tree", {
  g <- ga(sf::st_sfc(box(0, 0, 1, 1), sf::st_polygon(), box(2, 0, 3, 1)))
  idx <- RTree$new(g)

  expect_equal(idx$size(), 3L)
  expect_equal(idx$n_indexed(), 2L)
  expect_false(2L %in% conv(idx$search(-1, -1, 9, 9)))
})

test_that("row numbers survive a skipped geometry", {
  g <- ga(sf::st_sfc(sf::st_polygon(), box(2, 0, 3, 1)))
  idx <- RTree$new(g)

  expect_equal(conv(idx$search(2.5, 0.5, 2.6, 0.6)), 2L)
})

test_that("node_size is validated", {
  expect_error(RTree$new(ga(grid3()), node_size = 1), "node_size")
})

test_that("max_results is validated", {
  idx <- RTree$new(ga(grid3()))
  expect_error(idx$neighbors(0, 0, max_results = 0), "max_results")
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
    conv(small$search(-1, -1, 6, 2)),
    conv(large$search(-1, -1, 6, 2))
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
  hilbert <- RTree$new(g, sort = "hilbert")$query(g)
  str <- RTree$new(g, sort = "str")$query(g)

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
  hits <- as.vector(idx$query(g))

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

  from_polygons <- as.vector(idx$query(g))
  from_boxes <- as.vector(idx$query(ga_envelope(g)))
  from_points <- as.vector(idx$query(ga_centroid(g)))

  expect_equal(from_polygons, from_boxes)
  expect_length(from_points, 2L)
  expect_true(1 %in% from_points[[1]])
})

test_that("a null query geometry gives a null row", {
  g <- ga(sf::st_sfc(box(0, 0, 1, 1), box(5, 5, 6, 6)))
  idx <- RTree$new(g)
  res <- idx$query(ga(sf::st_sfc(box(0, 0, 1, 1), sf::st_polygon())))

  expect_equal(res$length, 2L)
  expect_equal(res$null_count, 1L)
})

test_that("query rows point back at the original array", {
  g <- ga(sf::st_sfc(box(0, 0, 1, 1), box(50, 50, 51, 51)))
  idx <- RTree$new(g)
  hits <- as.vector(idx$query(ga(sf::st_sfc(box(50, 50, 51, 51)))))

  expect_equal(hits[[1]], 2L)
})
