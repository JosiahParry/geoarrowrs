skip_if_not_installed("sf")
skip_if_not_installed("geoarrow")

nc_path <- system.file("shape/nc.shp", package = "sf")
nc <- as.data.frame(read_shapefile(nc_path))

test_that("ga_collect_agg returns a single geometry", {
  res <- ga_collect_agg(nc$geometry)
  expect_identical(res$length, 1L)
})

test_that("the collected hull matches sf", {
  nc_sf <- sf::st_read(nc_path, quiet = TRUE)
  cent <- suppressWarnings(sf::st_centroid(sf::st_set_crs(nc_sf, NA)))
  want <- as.numeric(sf::st_area(sf::st_convex_hull(sf::st_union(cent))))

  got <- as.vector(ga_unsigned_area(
    ga_convex_hull(ga_collect_agg(ga_centroid(nc$geometry)))
  ))
  expect_equal(got, want, tolerance = 1e-6)
})

test_that("ga_collect_agg keeps every non null row", {
  pts <- ga_xy(c(1, NA, 3), c(4, NA, 6))
  hull <- ga_convex_hull(ga_collect_agg(pts))
  expect_identical(hull$length, 1L)
  expect_identical(ga_collect_agg(pts)$null_count, 0L)
})

test_that("ga_collect_agg does not dissolve, unlike ga_unary_union", {
  sq <- function(x) {
    sf::st_polygon(list(rbind(
      c(x, 0),
      c(x + 2, 0),
      c(x + 2, 2),
      c(x, 2),
      c(x, 0)
    )))
  }
  # two squares overlapping by half
  g <- geoarrow::as_geoarrow_array(sf::st_sfc(sq(0), sq(1)))

  collected <- as.vector(ga_unsigned_area(ga_collect_agg(g)))
  dissolved <- as.vector(ga_unsigned_area(ga_unary_union(g)))

  expect_equal(collected, 8)
  expect_equal(dissolved, 6)
})

test_that("ga_collect_agg works per group, which is the point", {
  groups <- rep(c("a", "b"), each = 50)
  areas <- vapply(
    split(seq_len(100), groups),
    function(i) {
      as.vector(ga_unsigned_area(
        ga_convex_hull(ga_collect_agg(ga_centroid(nc$geometry[i])))
      ))
    },
    numeric(1)
  )
  expect_length(areas, 2L)
  expect_true(all(areas > 0))
})

test_that("sizes collects one geometry per group", {
  pts <- ga_centroid(nc$geometry)
  res <- ga_collect_agg(pts, sizes = rep(20, 5))

  expect_identical(res$length, 5L)
})

test_that("a group's hull is the hull of just that group", {
  pts <- geoarrow::as_geoarrow_vctr(ga_centroid(nc$geometry))
  grouped <- ga_collect_agg(pts, sizes = c(30, 70))

  want <- vapply(
    list(1:30, 31:100),
    function(i) {
      as.vector(ga_unsigned_area(ga_convex_hull(ga_collect_agg(
        pts[i]
      ))))
    },
    numeric(1)
  )
  expect_equal(as.vector(ga_unsigned_area(ga_convex_hull(grouped))), want)
})

test_that("one group of everything matches the ungrouped form", {
  pts <- ga_centroid(nc$geometry)

  expect_equal(
    as.vector(ga_unsigned_area(ga_convex_hull(ga_collect_agg(
      pts,
      sizes = 100
    )))),
    as.vector(ga_unsigned_area(ga_convex_hull(ga_collect_agg(pts))))
  )
})

test_that("a group of zero rows is an empty collection", {
  pts <- ga_centroid(nc$geometry)
  res <- ga_collect_agg(pts, sizes = c(50, 0, 50))

  expect_identical(res$length, 3L)
  expect_equal(as.vector(ga_unsigned_area(ga_convex_hull(res)))[2], 0)
})

test_that("sizes has to cover the array", {
  pts <- ga_centroid(nc$geometry)

  expect_error(ga_collect_agg(pts, sizes = c(10, 10)), "must sum to")
  expect_error(ga_collect_agg(pts, sizes = c(50, NA)), "must not contain NA")
  expect_error(ga_collect_agg(pts, sizes = c(100.5)), "whole")
  expect_error(ga_collect_agg(pts, sizes = c(-1, 101)), "not negative")
})
