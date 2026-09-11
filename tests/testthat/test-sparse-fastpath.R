# A point on either side of a sparse predicate is answered from where the point
# sits rather than from a DE-9IM matrix, which is a different code path. These
# pin it to the matrix it replaces, on the cases that distinguish them: points
# on a vertex, on an edge, and on an edge two polygons share.

PREDICATES <- c(
  "intersects",
  "contains",
  "contains_properly",
  "within",
  "covers",
  "covered_by",
  "touches",
  "crosses",
  "overlaps",
  "equals_topo"
)

skip_deps <- function() {
  skip_if_not_installed("sf")
  skip_if_not_installed("geoarrow")
}

#' The rows the pairwise predicate says match, one list per row of `x`
by_relate <- function(pair, xv, yv) {
  lapply(seq_along(xv), function(i) {
    as.integer(which(as.vector(pair(xv[rep(i, length(yv))], yv))))
  })
}

expect_agrees_with_relate <- function(xv, yv) {
  for (p in PREDICATES) {
    sparse <- get(paste0("ga_sparse_", p))
    pair <- get(paste0("ga_", p))
    expect_equal(
      lapply(as.vector(sparse(xv, yv)), as.integer),
      by_relate(pair, xv, yv),
      info = p
    )
  }
}

test_that("a point y agrees with the matrix it replaces", {
  skip_deps()
  sq <- function(x, y, s = 1) {
    sf::st_polygon(list(matrix(
      c(x, y, x + s, y, x + s, y + s, x, y + s, x, y),
      ncol = 2,
      byrow = TRUE
    )))
  }
  # squares sharing edges, so a point can sit on two boundaries at once
  xv <- geoarrow::as_geoarrow_vctr(geoarrow::as_geoarrow_array(
    sf::st_sfc(sq(0, 0), sq(1, 0), sq(0, 1), sq(1, 1), sq(3, 3))
  ))
  yv <- geoarrow::as_geoarrow_vctr(ga_xy(
    c(0, 1, 2, 0.5, 1, 1.5, 0.5, 1.5, 3, 9, 0, 1),
    c(0, 0, 0, 0, 0.5, 0.5, 0.5, 1.5, 3, 9, 1, 1)
  ))

  expect_agrees_with_relate(xv, yv)
})

test_that("a point x agrees with the matrix it replaces", {
  skip_deps()
  sq <- function(x, y, s = 1) {
    sf::st_polygon(list(matrix(
      c(x, y, x + s, y, x + s, y + s, x, y + s, x, y),
      ncol = 2,
      byrow = TRUE
    )))
  }
  polys <- geoarrow::as_geoarrow_vctr(geoarrow::as_geoarrow_array(
    sf::st_sfc(sq(0, 0), sq(1, 0), sq(0, 1), sq(1, 1), sq(3, 3))
  ))
  points <- geoarrow::as_geoarrow_vctr(ga_xy(
    c(0, 1, 2, 0.5, 1, 1.5, 0.5, 1.5, 3, 9, 0, 1),
    c(0, 0, 0, 0, 0.5, 0.5, 0.5, 1.5, 3, 9, 1, 1)
  ))

  expect_agrees_with_relate(points, polys)
})

test_that("multipolygons with boundary points agree with the matrix", {
  skip_deps()
  path <- system.file("shape/nc.shp", package = "sf")
  skip_if(path == "")

  g <- as.data.frame(read_shapefile(path))$geometry
  xv <- geoarrow::as_geoarrow_vctr(g)
  # the counties' own vertices, so many of the points land exactly on an edge
  co <- sf::st_coordinates(sf::st_as_sfc(g[1:12]))
  keep <- seq(1, nrow(co), by = 7)
  yv <- geoarrow::as_geoarrow_vctr(ga_xy(co[keep, "X"], co[keep, "Y"]))

  expect_agrees_with_relate(xv, yv)
})

test_that("a point against a line agrees with the matrix", {
  skip_deps()
  lines <- geoarrow::as_geoarrow_vctr(geoarrow::as_geoarrow_array(sf::st_sfc(
    sf::st_linestring(matrix(c(0, 0, 2, 2), ncol = 2, byrow = TRUE)),
    sf::st_linestring(matrix(c(0, 2, 2, 0), ncol = 2, byrow = TRUE))
  )))
  # an endpoint, the crossing, a midpoint, and a miss
  pts <- geoarrow::as_geoarrow_vctr(ga_xy(
    c(0, 1, 0.5, 5),
    c(0, 1, 0.5, 5)
  ))

  expect_agrees_with_relate(lines, pts)
  expect_agrees_with_relate(pts, lines)
})

test_that("many candidates take the indexed path and still agree", {
  skip_deps()
  # past a threshold the edges are indexed rather than walked, so this needs
  # enough points on one row to cross it
  ring <- matrix(
    c(0, 0, 4, 0, 4, 4, 0, 4, 0, 0),
    ncol = 2,
    byrow = TRUE
  )
  hole <- matrix(
    c(1, 1, 2, 1, 2, 2, 1, 2, 1, 1),
    ncol = 2,
    byrow = TRUE
  )
  xv <- geoarrow::as_geoarrow_vctr(geoarrow::as_geoarrow_array(sf::st_sfc(
    sf::st_polygon(list(ring)),
    sf::st_polygon(list(ring, hole)),
    sf::st_multipolygon(list(list(ring)))
  )))

  # a grid that lands inside, in the hole, on edges, on vertices and outside
  grid <- expand.grid(x = seq(-1, 5, by = 0.5), y = seq(-1, 5, by = 0.5))
  yv <- geoarrow::as_geoarrow_vctr(ga_xy(grid$x, grid$y))
  expect_gt(length(yv), 100L)

  expect_agrees_with_relate(xv, yv)
})

test_that("the indexed path handles a hole and a point in it", {
  skip_deps()
  ring <- matrix(c(0, 0, 4, 0, 4, 4, 0, 4, 0, 0), ncol = 2, byrow = TRUE)
  hole <- matrix(c(1, 1, 3, 1, 3, 3, 1, 3, 1, 1), ncol = 2, byrow = TRUE)
  donut <- geoarrow::as_geoarrow_array(sf::st_sfc(sf::st_polygon(list(
    ring,
    hole
  ))))

  # enough points to index: in the ring, in the hole, and on the hole's edge
  pts <- ga_xy(
    c(0.5, 2, 1, seq(0.1, 3.9, length.out = 20)),
    c(0.5, 2, 1, rep(0.5, 20))
  )
  got <- as.vector(ga_sparse_contains(donut, pts))[[1]]

  # the hole centre is not contained, the ring interior is
  expect_false(2L %in% as.integer(got))
  expect_true(1L %in% as.integer(got))
})

test_that("a canonically wound hole takes the index and agrees", {
  skip_deps()
  ring <- matrix(c(0, 0, 4, 0, 4, 4, 0, 4, 0, 0), ncol = 2, byrow = TRUE)
  hole <- matrix(c(1, 1, 3, 1, 3, 3, 1, 3, 1, 1), ncol = 2, byrow = TRUE)
  # ga_orient gives the winding the index assumes, so this is the indexed path
  donut <- ga_orient(
    geoarrow::as_geoarrow_array(sf::st_sfc(sf::st_polygon(list(ring, hole)))),
    "ccw"
  )
  xv <- geoarrow::as_geoarrow_vctr(donut)

  grid <- expand.grid(x = seq(-1, 5, by = 0.4), y = seq(-1, 5, by = 0.4))
  yv <- geoarrow::as_geoarrow_vctr(ga_xy(grid$x, grid$y))

  expect_agrees_with_relate(xv, yv)

  # and the hole really is excluded
  got <- as.integer(as.vector(ga_sparse_contains(
    xv,
    ga_xy(
      c(0.5, 2),
      c(
        0.5,
        2
      )
    )
  ))[[1]])
  expect_equal(got, 1L)
})

test_that("a null on either side still gives a null row", {
  skip_deps()
  polys <- geoarrow::as_geoarrow_array(sf::st_sfc(
    sf::st_polygon(list(matrix(
      c(0, 0, 1, 0, 1, 1, 0, 1, 0, 0),
      ncol = 2,
      byrow = TRUE
    ))),
    sf::st_polygon()
  ))
  pts <- ga_xy(c(0.5, NA), c(0.5, NA))

  got <- ga_sparse_contains(polys, pts)
  expect_equal(got$length, 2L)
  expect_equal(got$null_count, 1L)
  # the null point is never returned as a match
  expect_equal(as.integer(as.vector(got)[[1]]), 1L)
})
