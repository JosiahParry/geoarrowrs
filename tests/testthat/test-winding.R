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

ccw_ring <- matrix(c(0, 0, 1, 0, 1, 1, 0, 1, 0, 0), ncol = 2, byrow = TRUE)
cw_ring <- ccw_ring[rev(seq_len(nrow(ccw_ring))), ]

ccw_poly <- function() sf::st_polygon(list(ccw_ring))
cw_poly <- function() sf::st_polygon(list(cw_ring))

test_that("winding_order reports both directions", {
  g <- ga(sf::st_sfc(ccw_poly(), cw_poly()))
  expect_equal(conv(winding_order(g)), c("counterclockwise", "clockwise"))
})

test_that("winding_order preserves length and nulls unwindable geometries", {
  g <- ga(sf::st_sfc(ccw_poly(), sf::st_point(c(0, 0))))
  res <- conv(winding_order(g))

  expect_length(res, 2L)
  expect_equal(res[1], "counterclockwise")
  expect_true(is.na(res[2]))
})

test_that("winding_order reads a linestring directly", {
  ls <- sf::st_linestring(ccw_ring)
  expect_equal(conv(winding_order(ga(sf::st_sfc(ls)))), "counterclockwise")
})

test_that("orient rewinds to the requested direction", {
  g <- ga(sf::st_sfc(ccw_poly(), cw_poly()))

  expect_equal(
    conv(winding_order(orient(g, "default"))),
    rep("counterclockwise", 2)
  )
  expect_equal(
    conv(winding_order(orient(g, "reversed"))),
    rep("clockwise", 2)
  )
})

test_that("orient accepts ccw and cw as aliases", {
  g <- ga(sf::st_sfc(cw_poly()))
  expect_equal(conv(winding_order(orient(g, "ccw"))), "counterclockwise")
  expect_equal(conv(winding_order(orient(g, "cw"))), "clockwise")
})

test_that("orient does not change the area it covers", {
  g <- ga(sf::st_sfc(cw_poly()))
  before <- sf::st_as_sfc(geoarrow::as_geoarrow_vctr(g))
  after <- sf::st_as_sfc(geoarrow::as_geoarrow_vctr(orient(g, "default")))

  expect_equal(sf::st_area(before), sf::st_area(after))
})

test_that("orient preserves geometry type and length", {
  polys <- ga(sf::st_sfc(ccw_poly(), cw_poly()))
  res <- sf::st_as_sfc(geoarrow::as_geoarrow_vctr(orient(polys, "default")))
  expect_length(res, 2L)
  expect_setequal(as.character(sf::st_geometry_type(res)), "POLYGON")

  mp <- ga(sf::st_sfc(sf::st_multipolygon(list(list(cw_ring)))))
  res <- sf::st_as_sfc(geoarrow::as_geoarrow_vctr(orient(mp, "default")))
  expect_length(res, 1L)
  expect_setequal(as.character(sf::st_geometry_type(res)), "MULTIPOLYGON")
})

test_that("orient rejects an unknown direction", {
  g <- ga(sf::st_sfc(ccw_poly()))
  expect_error(orient(g, "widdershins"), "direction")
})

test_that("orient rejects geometries with no rings", {
  g <- ga(sf::st_sfc(sf::st_point(c(0, 0))))
  expect_error(orient(g, "default"), "polygons")
})

test_that("is_ccw and is_cw agree with winding_order", {
  g <- ga(sf::st_sfc(ccw_poly(), cw_poly()))

  expect_equal(conv(is_ccw(g)), c(TRUE, FALSE))
  expect_equal(conv(is_cw(g)), c(FALSE, TRUE))
})

test_that("is_ccw is NA rather than FALSE where there is no winding", {
  g <- ga(sf::st_sfc(sf::st_point(c(0, 0))))
  expect_true(is.na(conv(is_ccw(g))))
  expect_true(is.na(conv(is_cw(g))))
})

test_that("winding functions handle a null geometry", {
  g <- ga(sf::st_sfc(ccw_poly(), sf::st_polygon()))

  expect_length(conv(winding_order(g)), 2L)
  expect_true(is.na(conv(winding_order(g))[2]))
  expect_true(is.na(conv(is_ccw(g))[2]))
})

test_that("winding functions read a concrete array from a reader", {
  skip_deps()
  path <- system.file("shape/nc.shp", package = "sf")
  skip_if(path == "")

  g <- as.data.frame(read_shapefile(path))$geometry
  res <- conv(winding_order(g))

  expect_length(res, 100L)
  expect_true(all(res %in% c("clockwise", "counterclockwise")))
})
