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
tosfc <- function(x) sf::st_as_sfc(geoarrow::as_geoarrow_vctr(x))
coords <- function(x) unname(sf::st_coordinates(tosfc(x))[, 1:2, drop = FALSE])

hline <- function() sf::st_linestring(cbind(c(0, 10), c(0, 0)))
square <- function() {
  sf::st_polygon(list(matrix(
    c(0, 0, 4, 0, 4, 4, 0, 4, 0, 0),
    ncol = 2,
    byrow = TRUE
  )))
}
pt <- function(x, y) sf::st_point(c(x, y))

test_that("closest_point projects onto a line", {
  g <- ga(sf::st_sfc(hline()))
  p <- ga(sf::st_sfc(pt(4, 5)))

  expect_equal(coords(ga_closest_point(g, p)), matrix(c(4, 0), ncol = 2))
})

test_that("closest_point returns the intersection when the point is on the line", {
  g <- ga(sf::st_sfc(hline()))
  p <- ga(sf::st_sfc(pt(3, 0)))

  expect_equal(coords(ga_closest_point(g, p)), matrix(c(3, 0), ncol = 2))
})

test_that("closest_point clamps to the ends of the line", {
  g <- ga(sf::st_sfc(hline(), hline()))
  p <- ga(sf::st_sfc(pt(-5, 1), pt(99, 1)))

  expect_equal(coords(ga_closest_point(g, p)), matrix(c(0, 10, 0, 0), ncol = 2))
})

test_that("closest_point recycles a single point", {
  g <- ga(sf::st_sfc(hline(), hline(), hline()))
  p <- ga(sf::st_sfc(pt(4, 5)))

  expect_length(tosfc(ga_closest_point(g, p)), 3L)
})

test_that("closest_point rejects a mismatched point length", {
  g <- ga(sf::st_sfc(hline(), hline()))
  p <- ga(sf::st_sfc(pt(0, 0), pt(1, 1), pt(2, 2)))

  expect_error(ga_closest_point(g, p), "point")
})

test_that("closest_point_haversine differs from the planar result", {
  line <- sf::st_linestring(cbind(c(0, 10), c(0, 10)))
  g <- ga(sf::st_sfc(line))
  p <- ga(sf::st_sfc(pt(5, 6)))

  planar <- coords(ga_closest_point(g, p))
  spherical <- coords(ga_closest_point_haversine(g, p))

  expect_false(isTRUE(all.equal(planar, spherical)))
})

test_that("closest_point preserves length and nulls a null point", {
  g <- ga(sf::st_sfc(hline(), hline()))
  p <- ga(sf::st_sfc(pt(1, 1), sf::st_point()))
  res <- tosfc(ga_closest_point(g, p))

  expect_length(res, 2L)
  expect_true(sf::st_is_empty(res)[2])
})

test_that("interior_point lands inside a concave polygon", {
  u <- sf::st_polygon(list(matrix(
    c(0, 0, 3, 0, 3, 3, 2, 3, 2, 1, 1, 1, 1, 3, 0, 3, 0, 0),
    ncol = 2,
    byrow = TRUE
  )))
  g <- ga(sf::st_sfc(u))

  inside <- sf::st_intersects(
    tosfc(ga_interior_point(g)),
    sf::st_sfc(u),
    sparse = FALSE
  )
  expect_true(inside[1, 1])
})

test_that("interior_point preserves length", {
  g <- ga(sf::st_sfc(square(), hline(), pt(1, 1)))
  expect_length(tosfc(ga_interior_point(g)), 3L)
})

test_that("interior_point is empty for an empty geometry", {
  g <- ga(sf::st_sfc(square(), sf::st_polygon()))
  res <- tosfc(ga_interior_point(g))

  expect_length(res, 2L)
  expect_true(sf::st_is_empty(res)[2])
})

test_that("is_convex distinguishes convex from concave", {
  concave <- sf::st_polygon(list(matrix(
    c(0, 0, 4, 0, 4, 4, 2, 2, 0, 4, 0, 0),
    ncol = 2,
    byrow = TRUE
  )))
  g <- ga(sf::st_sfc(square(), concave))

  expect_equal(conv(ga_is_convex(g)), c(TRUE, FALSE))
})

test_that("is_convex reads a closed linestring ring", {
  ring <- sf::st_linestring(cbind(c(0, 1, 2, 0), c(0, 1, 0, 0)))
  expect_true(conv(ga_is_convex(ga(sf::st_sfc(ring)))))
})

test_that("is_convex is FALSE for an unclosed linestring", {
  open <- sf::st_linestring(cbind(c(0, 1, 2), c(0, 1, 0)))
  expect_false(conv(ga_is_convex(ga(sf::st_sfc(open)))))
})

test_that("is_convex is NA where convexity is undefined", {
  g <- ga(sf::st_sfc(square(), pt(0, 0)))
  res <- conv(ga_is_convex(g))

  expect_length(res, 2L)
  expect_true(res[1])
  expect_true(is.na(res[2]))
})

test_that("line_locate_point returns a fraction of the line", {
  g <- ga(sf::st_sfc(hline(), hline(), hline()))
  p <- ga(sf::st_sfc(pt(0, 0), pt(2.5, 0), pt(10, 0)))

  expect_equal(conv(ga_line_locate_point(g, p)), c(0, 0.25, 1))
})

test_that("line_locate_point projects an off-line point", {
  g <- ga(sf::st_sfc(hline()))
  p <- ga(sf::st_sfc(pt(5, 7)))

  expect_equal(conv(ga_line_locate_point(g, p)), 0.5)
})

test_that("line_locate_point recycles and validates length", {
  g <- ga(sf::st_sfc(hline(), hline()))

  expect_length(conv(ga_line_locate_point(g, ga(sf::st_sfc(pt(5, 0))))), 2L)
  expect_error(
    ga_line_locate_point(g, ga(sf::st_sfc(pt(0, 0), pt(1, 0), pt(2, 0)))),
    "point"
  )
})

test_that("line_locate_point is NA for a geometry with no length", {
  g <- ga(sf::st_sfc(hline(), square()))
  p <- ga(sf::st_sfc(pt(5, 0)))
  res <- conv(ga_line_locate_point(g, p))

  expect_equal(res[1], 0.5)
  expect_true(is.na(res[2]))
})

test_that("query functions read a concrete array from a reader", {
  skip_deps()
  path <- system.file("shape/nc.shp", package = "sf")
  skip_if(path == "")

  g <- as.data.frame(read_shapefile(path))$geometry

  expect_length(tosfc(ga_interior_point(g)), 100L)
  expect_length(conv(ga_is_convex(g)), 100L)
  expect_length(tosfc(ga_closest_point(g, ga(sf::st_sfc(pt(-79, 35))))), 100L)
})

test_that("pairwise functions reject a length mismatch rather than truncating", {
  two <- ga(sf::st_sfc(pt(0, 0), pt(3, 4)))
  one <- ga(sf::st_sfc(pt(0, 0)))

  expect_error(ga_dist_euclidean_pairwise(two, one), "same length")
  expect_error(ga_dist_haversine_pairwise(two, one), "same length")
  expect_error(ga_dist_geodesic_pairwise(two, one), "same length")
  expect_error(ga_dist_rhumb_pairwise(two, one), "same length")
  expect_error(ga_dist_vincenty_pairwise(two, one), "same length")
  expect_error(ga_bearing_euclidean(two, one), "same length")
  expect_error(ga_bearing_haversine(two, one), "same length")
  expect_error(ga_bearing_geodesic(two, one), "same length")
  expect_error(ga_bearing_rhumb(two, one), "same length")
})

test_that("the geometry pairwise distances check length too", {
  two <- ga(sf::st_sfc(square(), square()))
  one <- ga(sf::st_sfc(square()))
  two_lines <- ga(sf::st_sfc(hline(), hline()))
  one_line <- ga(sf::st_sfc(hline()))

  expect_error(ga_dist_hausdorff_pairwise(two, one), "same length")
  expect_error(ga_dist_frechet_pairwise(two_lines, one_line), "same length")
})

test_that("equal lengths still work", {
  two <- ga(sf::st_sfc(pt(0, 0), pt(3, 4)))
  origin <- ga(sf::st_sfc(pt(0, 0), pt(0, 0)))

  expect_equal(
    conv(ga_dist_euclidean_pairwise(two, origin)),
    c(0, 5)
  )
})
