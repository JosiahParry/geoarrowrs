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

ccw_ring <- matrix(c(0, 0, 1, 0, 1, 1, 0, 1, 0, 0), ncol = 2, byrow = TRUE)
cw_ring <- ccw_ring[rev(seq_len(nrow(ccw_ring))), ]

ccw_poly <- function() sf::st_polygon(list(ccw_ring))
cw_poly <- function() sf::st_polygon(list(cw_ring))

test_that("winding_order reports both directions", {
  g <- ga(sf::st_sfc(ccw_poly(), cw_poly()))
  expect_equal(conv(ga_winding_order(g)), c("counterclockwise", "clockwise"))
})

test_that("winding_order preserves length and nulls unwindable geometries", {
  g <- ga(sf::st_sfc(ccw_poly(), sf::st_point(c(0, 0))))
  res <- conv(ga_winding_order(g))

  expect_length(res, 2L)
  expect_equal(res[1], "counterclockwise")
  expect_true(is.na(res[2]))
})

test_that("winding_order reads a linestring directly", {
  ls <- sf::st_linestring(ccw_ring)
  expect_equal(conv(ga_winding_order(ga(sf::st_sfc(ls)))), "counterclockwise")
})

test_that("orient rewinds to the requested direction", {
  g <- ga(sf::st_sfc(ccw_poly(), cw_poly()))

  expect_equal(
    conv(ga_winding_order(ga_orient(g, "default"))),
    rep("counterclockwise", 2)
  )
  expect_equal(
    conv(ga_winding_order(ga_orient(g, "reversed"))),
    rep("clockwise", 2)
  )
})

test_that("orient accepts ccw and cw as aliases", {
  g <- ga(sf::st_sfc(cw_poly()))
  expect_equal(conv(ga_winding_order(ga_orient(g, "ccw"))), "counterclockwise")
  expect_equal(conv(ga_winding_order(ga_orient(g, "cw"))), "clockwise")
})

test_that("orient does not change the area it ga_covers", {
  g <- ga(sf::st_sfc(cw_poly()))
  before <- sf::st_as_sfc(geoarrow::as_geoarrow_vctr(g))
  after <- sf::st_as_sfc(geoarrow::as_geoarrow_vctr(ga_orient(g, "default")))

  expect_equal(sf::st_area(before), sf::st_area(after))
})

test_that("orient preserves geometry type and length", {
  polys <- ga(sf::st_sfc(ccw_poly(), cw_poly()))
  res <- sf::st_as_sfc(geoarrow::as_geoarrow_vctr(ga_orient(polys, "default")))
  expect_length(res, 2L)
  expect_setequal(as.character(sf::st_geometry_type(res)), "POLYGON")

  mp <- ga(sf::st_sfc(sf::st_multipolygon(list(list(cw_ring)))))
  res <- sf::st_as_sfc(geoarrow::as_geoarrow_vctr(ga_orient(mp, "default")))
  expect_length(res, 1L)
  expect_setequal(as.character(sf::st_geometry_type(res)), "MULTIPOLYGON")
})

test_that("orient rejects an unknown direction", {
  g <- ga(sf::st_sfc(ccw_poly()))
  expect_error(ga_orient(g, "widdershins"), "direction")
})

test_that("orient rejects geometries with no rings", {
  g <- ga(sf::st_sfc(sf::st_point(c(0, 0))))
  expect_error(ga_orient(g, "default"), "polygons")
})

test_that("is_ccw and is_cw agree with ga_winding_order", {
  g <- ga(sf::st_sfc(ccw_poly(), cw_poly()))

  expect_equal(conv(ga_is_ccw(g)), c(TRUE, FALSE))
  expect_equal(conv(ga_is_cw(g)), c(FALSE, TRUE))
})

test_that("is_ccw is NA rather than FALSE where there is no winding", {
  g <- ga(sf::st_sfc(sf::st_point(c(0, 0))))
  expect_true(is.na(conv(ga_is_ccw(g))))
  expect_true(is.na(conv(ga_is_cw(g))))
})

test_that("winding functions handle a null geometry", {
  g <- ga(sf::st_sfc(ccw_poly(), sf::st_polygon()))

  expect_length(conv(ga_winding_order(g)), 2L)
  expect_true(is.na(conv(ga_winding_order(g))[2]))
  expect_true(is.na(conv(ga_is_ccw(g))[2]))
})

test_that("winding functions read a concrete array from a reader", {
  skip_deps()
  path <- system.file("shape/nc.shp", package = "sf")
  skip_if(path == "")

  g <- as.data.frame(read_shapefile(path))$geometry
  res <- conv(ga_winding_order(g))

  expect_length(res, 100L)
  expect_true(all(res %in% c("clockwise", "counterclockwise")))
})

as_wkt <- function(x) {
  sf::st_as_text(sf::st_as_sfc(geoarrow::as_geoarrow_vctr(x)))
}

extension_name <- function(x) {
  nanoarrow::infer_nanoarrow_schema(x)$metadata[["ARROW:extension:name"]]
}

hole <- matrix(
  c(0.2, 0.2, 0.2, 0.4, 0.4, 0.4, 0.4, 0.2, 0.2, 0.2),
  ncol = 2,
  byrow = TRUE
)

test_that("reverse matches sf and keeps the type of each concrete array", {
  inputs <- list(
    sf::st_sfc(
      sf::st_linestring(rbind(c(0, 0), c(1, 1), c(2, 0))),
      sf::st_linestring(rbind(c(5, 5), c(6, 6)))
    ),
    sf::st_sfc(sf::st_polygon(list(ccw_ring, hole)), cw_poly()),
    sf::st_sfc(sf::st_multilinestring(list(
      rbind(c(0, 0), c(1, 1)),
      rbind(c(5, 5), c(6, 6))
    ))),
    sf::st_sfc(sf::st_multipolygon(list(
      list(ccw_ring, hole),
      list(ccw_ring + 10)
    )))
  )

  for (x in inputs) {
    g <- ga(x)
    res <- ga_reverse(g)
    expect_identical(extension_name(res), extension_name(g))
    expect_equal(as_wkt(res), sf::st_as_text(sf::st_reverse(x)))
  }
})

test_that("reverse matches sf on a mixed array with a collection", {
  x <- sf::st_sfc(
    sf::st_point(c(1, 2)),
    sf::st_linestring(rbind(c(0, 0), c(1, 1), c(2, 0))),
    ccw_poly(),
    sf::st_multipoint(rbind(c(0, 0), c(1, 1), c(2, 2))),
    sf::st_geometrycollection(list(
      sf::st_point(c(0, 0)),
      sf::st_linestring(rbind(c(0, 0), c(1, 1)))
    ))
  )
  g <- ga(x)
  res <- ga_reverse(g)

  expect_identical(extension_name(res), extension_name(g))
  expect_equal(as_wkt(res), sf::st_as_text(sf::st_reverse(x)))
})

test_that("reverse leaves points and multipoints as they are", {
  pts <- sf::st_sfc(sf::st_point(c(1, 2)), sf::st_point())
  mpts <- sf::st_sfc(sf::st_multipoint(rbind(c(0, 0), c(1, 1), c(2, 2))))

  expect_equal(as_wkt(ga_reverse(ga(pts))), sf::st_as_text(sf::st_reverse(pts)))
  expect_equal(
    as_wkt(ga_reverse(ga(mpts))),
    sf::st_as_text(sf::st_reverse(mpts))
  )
})

test_that("reverse flips polygon winding and undoes itself", {
  g <- ga(sf::st_sfc(ccw_poly(), cw_poly()))

  expect_equal(
    conv(ga_winding_order(ga_reverse(g))),
    c("clockwise", "counterclockwise")
  )
  expect_equal(as_wkt(ga_reverse(ga_reverse(g))), as_wkt(g))
})

test_that("reverse is length preserving and null propagating", {
  skip_deps()
  g <- geoarrow::as_geoarrow_array(
    wk::wkt(c("LINESTRING (0 0, 1 1)", NA, "LINESTRING EMPTY"))
  )
  res <- ga_reverse(g)

  expect_equal(res$length, 3L)
  expect_equal(conv(ga_is_empty(res)), c(FALSE, NA, TRUE))
})
