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
tosfc <- function(x) sf::st_as_sfc(geoarrow::as_geoarrow_vctr(x))

seg <- function(x1, y1, x2, y2) {
  sf::st_linestring(cbind(c(x1, x2), c(y1, y2)))
}

bowtie <- function() {
  sf::st_polygon(list(matrix(
    c(0, 0, 2, 2, 2, 0, 0, 2, 0, 0),
    ncol = 2,
    byrow = TRUE
  )))
}

square <- function() {
  sf::st_polygon(list(matrix(
    c(0, 0, 2, 0, 2, 2, 0, 2, 0, 0),
    ncol = 2,
    byrow = TRUE
  )))
}

test_that("crossing lines meet at a point", {
  x <- ga(sf::st_sfc(seg(0, 0, 2, 2)))
  y <- ga(sf::st_sfc(seg(0, 2, 2, 0)))
  res <- tosfc(ga_line_intersection(x, y))

  expect_setequal(as.character(sf::st_geometry_type(res)), "MULTIPOINT")
  expect_equal(nrow(res[[1]]), 1L)
  expect_equal(unname(res[[1]][1, ]), c(1, 1))
})

test_that("overlapping lines meet in a segment", {
  x <- ga(sf::st_sfc(seg(0, 0, 4, 0)))
  y <- ga(sf::st_sfc(seg(2, 0, 6, 0)))
  res <- tosfc(ga_line_intersection(x, y))

  expect_setequal(as.character(sf::st_geometry_type(res)), "MULTIPOINT")
  expect_equal(nrow(res[[1]]), 2L)
  expect_equal(sort(res[[1]][, 1]), c(2, 4))
})

test_that("n_coords distinguishes a crossing from an overlap", {
  x <- ga(sf::st_sfc(seg(0, 0, 2, 2), seg(0, 0, 4, 0)))
  y <- ga(sf::st_sfc(seg(0, 2, 2, 0), seg(2, 0, 6, 0)))

  expect_equal(conv(ga_n_coords(ga_line_intersection(x, y))), c(1L, 2L))
})

test_that("parallel lines give null", {
  x <- ga(sf::st_sfc(seg(0, 0, 2, 0)))
  y <- ga(sf::st_sfc(seg(0, 1, 2, 1)))
  res <- ga_line_intersection(x, y)

  expect_equal(res$length, 1L)
  expect_equal(res$null_count, 1L)
})

test_that("a longer linestring is not a single segment", {
  long <- sf::st_linestring(cbind(c(0, 1, 2), c(0, 1, 0)))
  x <- ga(sf::st_sfc(long))
  y <- ga(sf::st_sfc(seg(0, 2, 2, 0)))

  expect_equal(ga_line_intersection(x, y)$null_count, 1L)
})

test_that("line_intersection recycles and validates", {
  x <- ga(sf::st_sfc(seg(0, 0, 2, 2), seg(0, 0, 2, 2)))
  y1 <- ga(sf::st_sfc(seg(0, 2, 2, 0)))
  y3 <- ga(sf::st_sfc(seg(0, 0, 1, 1), seg(0, 0, 1, 1), seg(0, 0, 1, 1)))

  expect_length(tosfc(ga_line_intersection(x, y1)), 2L)
  expect_error(ga_line_intersection(x, y3), "y")
})

test_that("self_intersections finds the crossing in a bowtie", {
  res <- tosfc(ga_self_intersections(ga(sf::st_sfc(bowtie()))))

  expect_length(res, 1L)
  expect_gt(nrow(res[[1]]), 0L)
  expect_equal(unname(res[[1]][1, ]), c(1, 1))
})

test_that("a valid square has no self intersections", {
  res <- tosfc(ga_self_intersections(ga(sf::st_sfc(square()))))
  expect_true(sf::st_is_empty(res))
})

test_that("self_intersections complements ga_is_valid", {
  g <- ga(sf::st_sfc(square(), bowtie()))

  expect_equal(conv(ga_is_valid(g)), c(TRUE, FALSE))
  expect_equal(sf::st_is_empty(tosfc(ga_self_intersections(g))), c(TRUE, FALSE))
})

test_that("self_intersections is length preserving", {
  g <- ga(sf::st_sfc(bowtie(), square(), bowtie()))
  expect_length(tosfc(ga_self_intersections(g)), 3L)
})

test_that("a point has no segments to cross", {
  g <- ga(sf::st_sfc(bowtie(), sf::st_point(c(0, 0))))
  res <- ga_self_intersections(g)

  expect_equal(res$length, 2L)
  expect_equal(res$null_count, 1L)
})
