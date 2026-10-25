skip_if_not_installed("sf")
skip_if_not_installed("geoarrow")

nc_sf <- sf::st_geometry(sf::st_read(
  system.file("shape/nc.shp", package = "sf"),
  quiet = TRUE
))
nc <- geoarrow::as_geoarrow_array(nc_sf)

as_sfc <- function(x) sf::st_as_sfc(geoarrow::as_geoarrow_vctr(x))

# the same cells in the same order, down to the last bit of every coordinate
expect_same_grid <- function(got, want) {
  got <- as_sfc(got)
  expect_identical(
    unname(sf::st_coordinates(got)[, 1:2]),
    unname(sf::st_coordinates(want)[, 1:2])
  )
  expect_identical(sf::st_as_text(got), sf::st_as_text(want))
}

test_that("ga_make_grid matches sf over the extent of x", {
  expect_same_grid(ga_make_grid(nc), sf::st_make_grid(nc_sf))
  expect_same_grid(
    ga_make_grid(nc, n = c(4, 2)),
    sf::st_make_grid(nc_sf, n = c(4, 2))
  )
  expect_same_grid(
    ga_make_grid(nc, cellsize = 0.5),
    sf::st_make_grid(nc_sf, cellsize = 0.5)
  )
  expect_same_grid(
    ga_make_grid(nc, cellsize = c(1, 0.25)),
    sf::st_make_grid(nc_sf, cellsize = c(1, 0.25))
  )
})

test_that("ga_make_grid matches sf with offset, n, and cellsize combined", {
  expect_same_grid(
    ga_make_grid(nc, cellsize = 0.5, offset = c(-85, 33)),
    sf::st_make_grid(nc_sf, cellsize = 0.5, offset = c(-85, 33))
  )
  expect_same_grid(
    ga_make_grid(nc, cellsize = 0.5, n = c(3, 2)),
    sf::st_make_grid(nc_sf, cellsize = 0.5, n = c(3, 2))
  )
  expect_same_grid(
    ga_make_grid(nc, offset = c(-85, 33), n = c(3, 2)),
    sf::st_make_grid(nc_sf, offset = c(-85, 33), n = c(3, 2))
  )
  expect_same_grid(
    ga_make_grid(nc, cellsize = 0.5, offset = c(-85, 33), n = c(3, 2)),
    sf::st_make_grid(nc_sf, cellsize = 0.5, offset = c(-85, 33), n = c(3, 2))
  )
})

test_that("ga_make_grid needs no x given cellsize, offset, and n", {
  expect_same_grid(
    ga_make_grid(cellsize = 2, offset = c(0, 0), n = c(3, 4)),
    sf::st_make_grid(cellsize = 2, offset = c(0, 0), n = c(3, 4))
  )
  expect_error(ga_make_grid(cellsize = 2), "give `x`")
})

test_that("ga_make_grid with nothing gives sf's 10 degree globe", {
  res <- ga_make_grid()
  expect_same_grid(res, sf::st_make_grid())
  expect_equal(sf::st_crs(as_sfc(res)), sf::st_crs(4326))
})

test_that("ga_make_grid gives centres and corners, matching sf", {
  expect_same_grid(
    ga_make_grid(nc, what = "centers"),
    sf::st_make_grid(nc_sf, what = "centers")
  )
  expect_same_grid(
    ga_make_grid(nc, cellsize = 0.5, what = "centers"),
    sf::st_make_grid(nc_sf, cellsize = 0.5, what = "centers")
  )
  expect_same_grid(
    ga_make_grid(nc, n = c(4, 2), what = "corners"),
    sf::st_make_grid(nc_sf, n = c(4, 2), what = "corners")
  )
  expect_equal(
    as.vector(ga_geometry_type(ga_make_grid(nc, n = 2, what = "corners")))[1],
    "POINT"
  )
})

test_that("ga_make_grid records the crs of x unless given one", {
  # the round trip through Rust rewrites the PROJJSON's layout, not the CRS
  expect_equal(
    sf::st_crs(as_sfc(ga_make_grid(nc)))$epsg,
    sf::st_crs(nc_sf)$epsg
  )
  expect_equal(
    sf::st_crs(as_sfc(ga_make_grid(nc, crs = "EPSG:32119"))),
    sf::st_crs(32119)
  )
})

test_that("ga_make_grid rejects arguments it cannot use", {
  expect_error(ga_make_grid(nc, what = "hexes"), "`what` must be one of")
  expect_error(ga_make_grid(nc, n = c(0, 2)), "at least one cell")
  expect_error(ga_make_grid(nc, n = 2.5), "whole number")
  expect_error(ga_make_grid(nc, offset = 1), "`offset` must be 2 numbers")
  expect_error(ga_make_grid(nc, cellsize = c(1, 2, 3)), "`cellsize` must be")
})

test_that("the masked st_make_grid matches sf", {
  mask_sf()
  on.exit(detach("geoarrowrs:sf"), add = TRUE)
  geom <- geoarrow::as_geoarrow_vctr(nc)

  got <- st_make_grid(geom, n = c(4, 2))
  expect_s3_class(got, "geoarrow_vctr")
  expect_same_grid(got, sf::st_make_grid(nc_sf, n = c(4, 2)))
  expect_same_grid(
    st_make_grid(geom, cellsize = 0.5, what = "centers"),
    sf::st_make_grid(nc_sf, cellsize = 0.5, what = "centers")
  )
  expect_error(st_make_grid(geom, square = FALSE), "only square grids")
})
