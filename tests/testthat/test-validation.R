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

good <- function() {
  sf::st_polygon(list(matrix(
    c(0, 0, 2, 0, 2, 2, 0, 2, 0, 0),
    ncol = 2,
    byrow = TRUE
  )))
}

bowtie <- function() {
  sf::st_polygon(list(matrix(
    c(0, 0, 2, 2, 2, 0, 0, 2, 0, 0),
    ncol = 2,
    byrow = TRUE
  )))
}

test_that("is_valid separates good from self intersecting", {
  g <- ga(sf::st_sfc(good(), bowtie()))
  expect_equal(conv(is_valid(g)), c(TRUE, FALSE))
})

test_that("is_valid agrees with sf on the same geometries", {
  sfc <- sf::st_sfc(good(), bowtie())
  expect_equal(conv(is_valid(ga(sfc))), sf::st_is_valid(sfc))
})

test_that("validation_error explains only the invalid one", {
  g <- ga(sf::st_sfc(good(), bowtie()))
  res <- conv(validation_error(g))

  expect_length(res, 2L)
  expect_true(is.na(res[1]))
  expect_false(is.na(res[2]))
  expect_gt(nchar(res[2]), 0L)
})

test_that("a simple point is valid", {
  expect_true(conv(is_valid(ga(sf::st_sfc(sf::st_point(c(0, 0)))))))
})

test_that("validation is length preserving", {
  g <- ga(sf::st_sfc(good(), bowtie(), good()))

  expect_length(conv(is_valid(g)), 3L)
  expect_length(conv(validation_error(g)), 3L)
})

test_that("a null geometry gives NA", {
  g <- ga(sf::st_sfc(good(), sf::st_polygon()))
  res <- conv(is_valid(g))

  expect_length(res, 2L)
  expect_true(res[1])
})

test_that("validation reads a concrete array from a reader", {
  skip_deps()
  path <- system.file("shape/nc.shp", package = "sf")
  skip_if(path == "")

  g <- as.data.frame(read_shapefile(path))$geometry
  res <- conv(is_valid(g))

  expect_length(res, 100L)
  expect_type(res, "logical")
})
