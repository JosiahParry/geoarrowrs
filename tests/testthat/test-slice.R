skip_if_not_installed("sf")
skip_if_not_installed("geoarrow")

nc <- as.data.frame(read_shapefile(
  system.file("shape/nc.shp", package = "sf")
))
g <- nc$geometry
areas <- as.vector(ga_signed_area(g))

test_that("a contiguous slice is honoured", {
  expect_identical(ga_signed_area(g[1:3])$length, 3L)
  expect_identical(as.vector(ga_signed_area(g[50:52])), areas[50:52])
})

test_that("a single row is honoured", {
  expect_identical(as.vector(ga_signed_area(g[7])), areas[7])
})

test_that("a reordered slice keeps its order", {
  i <- c(5L, 1L, 3L)
  expect_identical(as.vector(ga_signed_area(g[i])), areas[i])
})

test_that("a repeated row is repeated", {
  expect_identical(as.vector(ga_signed_area(g[c(2L, 2L)])), areas[c(2, 2)])
})

test_that("an NA row is a null element", {
  expect_identical(as.vector(ga_signed_area(g[NA_integer_])), NA_real_)
})

test_that("an empty slice gives an empty result", {
  expect_identical(ga_signed_area(g[0])$length, 0L)
})

test_that("a slice reaches every kind of argument", {
  expect_identical(ga_centroid(g[1:3])$length, 3L)
  expect_identical(ga_sparse_intersects(g[1:3], g)$length, 3L)
  expect_identical(ga_intersects(g[1:3], g[1:3])$length, 3L)
})
