skip_deps <- function() {
  skip_if_not_installed("sf")
  skip_if_not_installed("geoarrow")
}

nc <- function() {
  skip_deps()
  as.data.frame(
    read_shapefile(system.file("shape/nc.shp", package = "sf"))
  )$geometry
}

test_that("hilbert order is a permutation of every row", {
  g <- nc()
  ord <- as.vector(ga_hilbert_order(g))

  expect_type(ord, "integer")
  expect_setequal(ord, seq_len(100))
  expect_length(ord, 100L)
})

test_that("hilbert order steps between adjacent cells of the grid", {
  skip_deps()
  # a corner at 65535 makes the grid's cells exactly the integer coordinates
  grid <- expand.grid(x = 0:3, y = 0:3)
  x <- c(grid$x, 65535)
  y <- c(grid$y, 65535)

  ord <- as.vector(ga_hilbert_order(ga_xy(x, y)))
  walk <- ord[ord <= 16]
  steps <- sqrt(diff(x[walk])^2 + diff(y[walk])^2)

  expect_identical(walk, ord[1:16])
  expect_true(all(steps == 1))
})

test_that("hilbert order puts neighbouring rows next to each other", {
  g <- nc()
  centres <- ga_centroid(g)
  x <- as.vector(ga_x(centres))
  y <- as.vector(ga_y(centres))
  gap <- function(o) mean(sqrt(diff(x[o])^2 + diff(y[o])^2))

  expect_lt(gap(as.vector(ga_hilbert_order(g))), gap(seq_along(x)) / 2)
})

test_that("hilbert order keeps ties in row order and puts nulls last", {
  skip_deps()
  g <- geoarrow::as_geoarrow_array(wk::wkt(c(
    "POINT (1 1)",
    NA,
    "POINT (1 1)",
    "POINT (0 0)",
    "POINT EMPTY"
  )))
  ord <- as.vector(ga_hilbert_order(g))

  expect_equal(ord[1:3], c(4L, 1L, 3L))
  expect_setequal(ord[4:5], c(2L, 5L))
})
