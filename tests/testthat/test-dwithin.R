skip_deps <- function() {
  skip_if_not_installed("sf")
  skip_if_not_installed("geoarrow")
}

boxes3 <- function() {
  skip_deps()
  sq <- function(x) {
    sf::st_polygon(list(matrix(
      c(x, 0, x + 1, 0, x + 1, 1, x, 1, x, 0),
      ncol = 2,
      byrow = TRUE
    )))
  }
  geoarrow::as_geoarrow_array(sf::st_sfc(sq(0), sq(2), sq(4)))
}

test_that("dwithin finds the rows inside the radius", {
  g <- boxes3()
  # box 1 is 1 away from box 2 and 3 away from box 3
  got <- as.vector(ga_sparse_dwithin(g, g, 1))

  expect_equal(as.integer(got[[1]]), c(1L, 2L))
  expect_equal(as.integer(got[[2]]), c(1L, 2L, 3L))
})

test_that("dwithin measures to the geometry, not to its box", {
  skip_deps()
  # an L shape whose box corner is much nearer the point than the shape is
  el <- geoarrow::as_geoarrow_array(sf::st_sfc(sf::st_polygon(list(matrix(
    c(0, 0, 10, 0, 10, 1, 1, 1, 1, 10, 0, 10, 0, 0),
    ncol = 2,
    byrow = TRUE
  )))))
  pt <- ga_xy(9, 9)

  expect_length(as.vector(ga_sparse_dwithin(pt, el, 7))[[1]], 0L)
  expect_equal(as.integer(as.vector(ga_sparse_dwithin(pt, el, 8))[[1]]), 1L)
})

test_that("dwithin agrees with measuring every pair", {
  skip_deps()
  path <- system.file("shape/nc.shp", package = "sf")
  skip_if(path == "")

  g <- as.data.frame(read_shapefile(path))$geometry
  xv <- geoarrow::as_geoarrow_vctr(ga_xy(
    c(-78.6, -80.8, -77.0),
    c(35.8, 35.2, 34.9)
  ))
  got <- as.vector(ga_sparse_dwithin(xv, g, 0.3))

  for (i in seq_along(xv)) {
    d <- as.vector(ga_dist_euclidean_pairwise(g, xv[i]))
    expect_equal(as.integer(got[[i]]), which(d <= 0.3))
  }
})

test_that("dwithin recycles a radius per row", {
  g <- boxes3()
  got <- as.vector(ga_sparse_dwithin(g, g, c(0, 1, 3)))

  expect_equal(as.integer(got[[1]]), 1L)
  expect_equal(as.integer(got[[2]]), c(1L, 2L, 3L))
  expect_equal(as.integer(got[[3]]), c(1L, 2L, 3L))
})

test_that("a zero radius still matches a row touching itself", {
  g <- boxes3()
  got <- as.vector(ga_sparse_dwithin(g, g, 0))
  expect_equal(as.integer(got[[1]]), 1L)
})

test_that("a null geometry or radius gives a null element", {
  skip_deps()
  g <- boxes3()
  q <- geoarrow::as_geoarrow_array(sf::st_sfc(
    sf::st_point(c(0.5, 0.5)),
    sf::st_polygon()
  ))

  expect_equal(ga_sparse_dwithin(q, g, 1)$null_count, 1L)
  expect_equal(ga_sparse_dwithin(g, g, c(1, NA, 1))$null_count, 1L)
})

test_that("a mismatched radius length is an error", {
  g <- boxes3()
  expect_error(ga_sparse_dwithin(g, g, c(1, 2)), "length 1")
})

test_that("dwithin is not the same as buffering and intersecting", {
  skip_deps()
  # a buffer approximates its arcs with segments, so the two disagree just
  # inside the radius; the distance test is the one that matches ST_DWithin
  g <- geoarrow::as_geoarrow_array(sf::st_sfc(sf::st_point(c(0, 0))))
  ring <- ga_xy(
    cos(seq(0, 2 * pi, length.out = 64)),
    sin(seq(
      0,
      2 * pi,
      length.out = 64
    ))
  )

  exact <- lengths(as.vector(ga_sparse_dwithin(g, ring, 1)))
  buffered <- lengths(as.vector(ga_sparse_intersects(ga_buffer(g, 1), ring)))

  expect_equal(exact, 64L)
  expect_lt(buffered, exact)
})
