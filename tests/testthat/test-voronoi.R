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

tosfc <- function(x) sf::st_as_sfc(geoarrow::as_geoarrow_vctr(x))

sites <- function() {
  sf::st_multipoint(cbind(c(0, 1, 1, 0), c(0, 0, 1, 1)))
}

unit_square <- function() {
  sf::st_polygon(list(matrix(
    c(0, 0, 1, 0, 1, 1, 0, 1, 0, 0),
    ncol = 2,
    byrow = TRUE
  )))
}

test_that("voronoi_cells returns one cell per site", {
  res <- tosfc(voronoi_cells(ga(sf::st_sfc(sites()))))

  expect_length(res, 1L)
  expect_equal(lengths(res), 4L)
  expect_setequal(as.character(sf::st_geometry_type(res)), "MULTIPOLYGON")
})

test_that("voronoi_edges returns a multilinestring", {
  res <- tosfc(voronoi_edges(ga(sf::st_sfc(sites()))))

  expect_length(res, 1L)
  expect_equal(lengths(res), 5L)
  expect_setequal(as.character(sf::st_geometry_type(res)), "MULTILINESTRING")
})

test_that("voronoi_edges scales with the number of sites", {
  three <- ga(sf::st_sfc(sf::st_multipoint(cbind(c(0, 1, 0.5), c(0, 0, 1)))))
  grid <- ga(sf::st_sfc(sf::st_multipoint(cbind(
    c(0, 1, 2, 0, 1),
    c(0, 0, 0, 1, 1)
  ))))

  expect_equal(lengths(tosfc(voronoi_edges(three))), 3L)
  expect_equal(lengths(tosfc(voronoi_edges(grid))), 7L)
})

test_that("voronoi_edges is empty for a single site", {
  one <- ga(sf::st_sfc(sf::st_point(c(0, 0))))
  expect_true(sf::st_is_empty(tosfc(voronoi_edges(one))))
})

test_that("voronoi_cells is length preserving", {
  g <- ga(sf::st_sfc(sites(), sites(), sites()))
  expect_length(tosfc(voronoi_cells(g)), 3L)
})

test_that("voronoi_edges is length preserving", {
  g <- ga(sf::st_sfc(sites(), sites(), sites()))
  expect_length(tosfc(voronoi_edges(g)), 3L)
})

test_that("envelope clips tighter than padded", {
  g <- ga(sf::st_sfc(sites()))
  padded <- sum(sf::st_area(tosfc(voronoi_cells(g, "padded"))))
  envelope <- sum(sf::st_area(tosfc(voronoi_cells(g, "envelope"))))

  expect_lt(envelope, padded)
  expect_equal(as.numeric(envelope), 1, tolerance = 1e-8)
})

test_that("boundary clips the diagram to a polygon", {
  g <- ga(sf::st_sfc(sites()))
  b <- ga(sf::st_sfc(unit_square()))
  clipped <- sum(sf::st_area(tosfc(voronoi_cells(g, boundary = b))))

  expect_equal(as.numeric(clipped), 1, tolerance = 1e-8)
})

test_that("boundary overrides the named clip mode", {
  g <- ga(sf::st_sfc(sites()))
  b <- ga(sf::st_sfc(unit_square()))
  with_padded <- sum(sf::st_area(tosfc(voronoi_cells(
    g,
    "padded",
    boundary = b
  ))))

  expect_equal(as.numeric(with_padded), 1, tolerance = 1e-8)
})

test_that("collinear sites yield no cells but do yield edges", {
  col <- ga(sf::st_sfc(sf::st_multipoint(cbind(c(0, 1, 2), c(0, 0, 0)))))

  expect_true(sf::st_is_empty(tosfc(voronoi_cells(col))))
  expect_false(sf::st_is_empty(tosfc(voronoi_edges(col))))
})

test_that("degenerate input is empty rather than dropped", {
  g <- ga(sf::st_sfc(sites(), sf::st_multipoint()))
  res <- tosfc(voronoi_cells(g))

  expect_length(res, 2L)
  expect_equal(sf::st_is_empty(res), c(FALSE, TRUE))
})

test_that("tolerance recycles and rejects a bad length", {
  g <- ga(sf::st_sfc(sites(), sites()))

  expect_length(tosfc(voronoi_cells(g, tolerance = 0)), 2L)
  expect_length(tosfc(voronoi_cells(g, tolerance = c(0, 0.01))), 2L)
  expect_error(voronoi_cells(g, tolerance = c(0, 0.1, 0.2)), "tolerance")
})

test_that("clip mode is validated", {
  g <- ga(sf::st_sfc(sites()))
  expect_error(voronoi_cells(g, "nope"), "clip")
  expect_error(voronoi_edges(g, "nope"), "clip")
})

test_that("voronoi reads a concrete array from a reader", {
  skip_deps()
  path <- system.file("shape/nc.shp", package = "sf")
  skip_if(path == "")

  g <- as.data.frame(read_shapefile(path))$geometry
  res <- tosfc(voronoi_cells(g))

  expect_length(res, 100L)
})
