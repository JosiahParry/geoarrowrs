skip_if_not_installed("sf")
skip_if_not_installed("geoarrow")

nc_sfc <- sf::st_geometry(sf::read_sf(system.file(
  "shape/nc.shp",
  package = "sf"
)))
nc <- geoarrow::as_geoarrow_array(nc_sfc)
planar_sfc <- sf::st_transform(nc_sfc, 32119)
planar <- geoarrow::as_geoarrow_array(planar_sfc)

as_sfc <- function(x) sf::st_as_sfc(geoarrow::as_geoarrow_vctr(x))

# the distance each row moved, read from its first coordinate
offsets <- function(moved, from) {
  first <- function(g) sf::st_coordinates(g)[1, 1:2]
  t(mapply(function(a, b) first(a) - first(b), as_sfc(moved), from))
}

# sf's own default amount for planar coordinates
sf_amount <- function(x, factor = 0.002) {
  bb <- sf::st_bbox(x)
  unname(factor * sqrt(diff(bb[c(3, 1)])^2 + diff(bb[c(4, 2)])^2))
}

test_that("ga_jitter moves each point by at most amount along each axis", {
  withr::local_seed(1)
  px <- stats::runif(1000)
  py <- stats::runif(1000)
  moved <- ga_jitter(ga_point(px, py), amount = 0.5, seed = 3)

  dx <- as.vector(ga_x(moved)) - px
  dy <- as.vector(ga_y(moved)) - py
  expect_true(all(abs(c(dx, dy)) <= 0.5))
  # uniform draws reach close to both ends of the range
  expect_true(all(c(min(dx), min(dy)) < -0.49 & c(max(dx), max(dy)) > 0.49))
})

test_that("ga_jitter moves a polygon as a whole and keeps its type", {
  moved <- ga_jitter(nc, amount = 0.1, seed = 1)
  moved_sfc <- as_sfc(moved)

  expect_equal(
    unique(as.vector(ga_geometry_type(moved))),
    "MULTIPOLYGON"
  )
  # every coordinate of a row moves by that row's offset
  shift <- sf::st_coordinates(moved_sfc)[, 1:2] -
    sf::st_coordinates(nc_sfc)[, 1:2]
  row <- sf::st_coordinates(nc_sfc)[, "L3"]
  expect_true(all(tapply(shift[, 1], row, function(v) diff(range(v))) < 1e-12))
  expect_true(all(tapply(shift[, 2], row, function(v) diff(range(v))) < 1e-12))
  # planar, since sf's spherical area changes as a shape moves north
  expect_equal(
    as.vector(ga_unsigned_area(moved)),
    as.vector(ga_unsigned_area(nc))
  )
})

test_that("ga_jitter takes sf's default amount", {
  amount <- sf_amount(planar_sfc)
  shift <- offsets(ga_jitter(planar, seed = 1), planar_sfc)
  expect_true(all(abs(shift) <= amount))
  expect_gt(min(apply(abs(shift), 2, max)), 0.9 * amount)

  amount <- sf_amount(planar_sfc, factor = 0.01)
  shift <- offsets(ga_jitter(planar, factor = 0.01, seed = 1), planar_sfc)
  expect_true(all(abs(shift) <= amount))
  expect_gt(min(apply(abs(shift), 2, max)), 0.9 * amount)
})

test_that("a seed repeats the offsets, and set.seed() repeats the default seed", {
  pts <- ga_point(c(0, 1, 2), c(0, 1, 2))
  x_of <- function(...) as.vector(ga_x(ga_jitter(pts, ...)))

  expect_identical(x_of(seed = 7), x_of(seed = 7))
  expect_false(identical(x_of(seed = 7), x_of(seed = 8)))

  first <- withr::with_seed(11, x_of())
  second <- withr::with_seed(11, x_of())
  expect_identical(first, second)
})

test_that("ga_jitter recycles amount and gives null where it cannot move a row", {
  pts <- ga_point(c(0, 1, 2, NA, 4), c(0, 1, 2, 3, 4))

  moved <- ga_jitter(pts, amount = c(1, NA, -1, 1, Inf), seed = 1)
  expect_equal(
    as.vector(ga_geometry_type(moved)),
    c("POINT", NA, NA, NA, NA)
  )

  still <- ga_jitter(pts, amount = 0, seed = 1)
  expect_equal(as.vector(ga_x(still)), c(0, 1, 2, NA, 4))

  expect_error(
    ga_jitter(pts, amount = c(1, 2), seed = 1),
    "`amount` must be length 1"
  )
  expect_error(ga_jitter(pts, seed = NA))
  expect_error(ga_jitter(pts, seed = -1))
})

test_that("ga_jitter keeps an empty polygon empty", {
  polys <- geoarrow::as_geoarrow_array(wk::wkt(c(
    "POLYGON EMPTY",
    "POLYGON ((0 0, 1 0, 1 1, 0 0))"
  )))
  expect_equal(
    as.vector(ga_is_empty(ga_jitter(polys, seed = 1))),
    c(TRUE, FALSE)
  )

  # with no coordinates there is no box to size the default amount, so nothing moves
  empty <- geoarrow::as_geoarrow_array(wk::wkt("POLYGON EMPTY"))
  expect_true(as.vector(ga_is_empty(ga_jitter(empty, seed = 1))))
})

test_that("the masked st_jitter calls ga_jitter", {
  mask_sf()
  on.exit(detach("geoarrowrs:sf"), add = TRUE)
  geom <- geoarrow::as_geoarrow_vctr(planar)

  got <- withr::with_seed(5, st_jitter(geom))
  want <- withr::with_seed(5, ga_jitter(planar))
  expect_s3_class(got, "geoarrow_vctr")
  expect_identical(as.vector(ga_x(got)), as.vector(ga_x(want)))

  got <- withr::with_seed(5, st_jitter(geom, amount = 100))
  want <- withr::with_seed(5, ga_jitter(planar, amount = 100))
  expect_identical(as.vector(ga_x(got)), as.vector(ga_x(want)))
})
