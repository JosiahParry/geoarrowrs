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
