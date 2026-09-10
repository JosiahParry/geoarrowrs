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

zigzag <- function() {
  sf::st_linestring(cbind(c(0, 1, 2, 3, 4), c(0, 0.1, 0, 0.1, 0)))
}

test_that("simplify_idx keeps the endpoints", {
  res <- lst(ga_simplify_idx(ga(sf::st_sfc(zigzag())), 0.5))[[1]]

  expect_equal(res[1], 1L)
  expect_equal(res[length(res)], 5L)
})

test_that("a larger epsilon keeps fewer coordinates", {
  g <- ga(sf::st_sfc(zigzag()))

  fine <- lst(ga_simplify_idx(g, 0.001))[[1]]
  coarse <- lst(ga_simplify_idx(g, 0.5))[[1]]

  expect_gt(length(fine), length(coarse))
})

test_that("indices are increasing and within range", {
  res <- lst(ga_simplify_idx(ga(sf::st_sfc(zigzag())), 0.05))[[1]]

  expect_false(is.unsorted(res))
  expect_true(all(res >= 1L & res <= 5L))
})

test_that("the kept indices match what simplify returns", {
  g <- ga(sf::st_sfc(zigzag()))
  idx <- lst(ga_simplify_idx(g, 0.5))[[1]]
  simplified <- sf::st_as_sfc(geoarrow::as_geoarrow_vctr(ga_simplify(g, 0.5)))

  expect_equal(length(idx), nrow(simplified[[1]]))
})

test_that("simplify_vw_idx also keeps the endpoints", {
  res <- lst(ga_simplify_vw_idx(ga(sf::st_sfc(zigzag())), 0.5))[[1]]

  expect_equal(res[1], 1L)
  expect_equal(res[length(res)], 5L)
})

test_that("both variants are length preserving", {
  g <- ga(sf::st_sfc(zigzag(), zigzag(), zigzag()))

  expect_length(lst(ga_simplify_idx(g, 0.5)), 3L)
  expect_length(lst(ga_simplify_vw_idx(g, 0.5)), 3L)
})

test_that("epsilon recycles and is validated", {
  g <- ga(sf::st_sfc(zigzag(), zigzag()))

  expect_length(lst(ga_simplify_idx(g, c(0.01, 0.5))), 2L)
  expect_error(ga_simplify_idx(g, c(0.1, 0.2, 0.3)), "epsilon")
})

test_that("a non linestring comes back null", {
  square <- sf::st_polygon(list(matrix(
    c(0, 0, 1, 0, 1, 1, 0, 1, 0, 0),
    ncol = 2,
    byrow = TRUE
  )))
  res <- ga_simplify_idx(ga(sf::st_sfc(zigzag(), square)), 0.5)

  expect_equal(res$length, 2L)
  expect_equal(res$null_count, 1L)
})
