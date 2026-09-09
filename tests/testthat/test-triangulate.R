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

# a square, which any triangulation splits into two triangles
square <- function(o = 0) {
  sf::st_polygon(list(matrix(
    c(o, o, o + 1, o, o + 1, o + 1, o, o + 1, o, o),
    ncol = 2, byrow = TRUE
  )))
}

n_parts <- function(res) {
  lengths(sf::st_as_sfc(geoarrow::as_geoarrow_vctr(res)))
}

test_that("earcut preserves length, one multipolygon per input", {
  g <- ga(sf::st_sfc(square(0), square(5), square(10)))
  res <- triangulate_earcut(g)
  sfc <- sf::st_as_sfc(geoarrow::as_geoarrow_vctr(res))

  expect_length(sfc, 3L)
  expect_setequal(as.character(sf::st_geometry_type(sfc)), "MULTIPOLYGON")
})

test_that("earcut splits a square into two triangles", {
  g <- ga(sf::st_sfc(square()))
  sfc <- sf::st_as_sfc(geoarrow::as_geoarrow_vctr(triangulate_earcut(g)))

  expect_equal(length(sfc[[1]]), 2L)
  # every part is a triangle: 4 coords, first == last
  for (part in sfc[[1]]) {
    expect_equal(nrow(part[[1]]), 4L)
  }
})

test_that("earcut area equals the input area", {
  g <- ga(sf::st_sfc(square()))
  sfc <- sf::st_as_sfc(geoarrow::as_geoarrow_vctr(triangulate_earcut(g)))
  expect_equal(sf::st_area(sfc[[1]]), 1, tolerance = 1e-9)
})

test_that("earcut nulls geometry types it cannot handle", {
  g <- ga(sf::st_sfc(sf::st_point(c(0, 0)), sf::st_point(c(1, 1))))
  sfc <- sf::st_as_sfc(geoarrow::as_geoarrow_vctr(triangulate_earcut(g)))

  expect_length(sfc, 2L)
  expect_true(all(sf::st_is_empty(sfc)))
})

test_that("earcut handles a multipolygon by triangulating each part", {
  mp <- sf::st_multipolygon(list(square(0), square(5)))
  g <- ga(sf::st_sfc(mp))
  sfc <- sf::st_as_sfc(geoarrow::as_geoarrow_vctr(triangulate_earcut(g)))

  expect_length(sfc, 1L)
  expect_equal(length(sfc[[1]]), 4L)
})

test_that("delaunay preserves length", {
  g <- ga(sf::st_sfc(square(0), square(5)))
  sfc <- sf::st_as_sfc(geoarrow::as_geoarrow_vctr(triangulate_delaunay(g)))
  expect_length(sfc, 2L)
})

test_that("delaunay defaults to constrained", {
  g <- ga(sf::st_sfc(square()))
  a <- sf::st_as_sfc(geoarrow::as_geoarrow_vctr(triangulate_delaunay(g)))
  b <- sf::st_as_sfc(geoarrow::as_geoarrow_vctr(triangulate_delaunay(g, TRUE)))
  expect_equal(length(a[[1]]), length(b[[1]]))
})

test_that("constrained triangulation of a square covers its area", {
  g <- ga(sf::st_sfc(square()))
  sfc <- sf::st_as_sfc(geoarrow::as_geoarrow_vctr(triangulate_delaunay(g)))
  expect_equal(sf::st_area(sfc[[1]]), 1, tolerance = 1e-9)
})

test_that("unconstrained triangulation ignores holes, constrained does not", {
  # a square with a square hole
  outer <- matrix(c(0, 0, 10, 0, 10, 10, 0, 10, 0, 0), ncol = 2, byrow = TRUE)
  hole <- matrix(c(3, 3, 7, 3, 7, 7, 3, 7, 3, 3), ncol = 2, byrow = TRUE)
  g <- ga(sf::st_sfc(sf::st_polygon(list(outer, hole))))

  con <- sf::st_as_sfc(geoarrow::as_geoarrow_vctr(triangulate_delaunay(g, TRUE)))
  unc <- sf::st_as_sfc(geoarrow::as_geoarrow_vctr(triangulate_delaunay(g, FALSE)))

  # constrained excludes the hole (area 100 - 16), unconstrained fills it
  expect_equal(sf::st_area(con[[1]]), 84, tolerance = 1e-6)
  expect_equal(sf::st_area(unc[[1]]), 100, tolerance = 1e-6)
})

test_that("snap_radius recycles and rejects a wrong length", {
  g <- ga(sf::st_sfc(square(0), square(5)))

  expect_no_error(triangulate_delaunay(g, TRUE, 1e-4))
  expect_no_error(triangulate_delaunay(g, TRUE, c(1e-4, 1e-3)))
  expect_error(
    triangulate_delaunay(g, TRUE, c(1e-4, 1e-3, 1e-2)),
    "length 1 or the same length"
  )
})

test_that("triangulation works on a shapefile read from disk", {
  skip_deps()
  requireNamespace("geoarrow", quietly = TRUE)

  g <- as.data.frame(
    read_shapefile(system.file("shape/nc.shp", package = "sf"))
  )$geometry

  res <- triangulate_earcut(g)
  expect_length(sf::st_as_sfc(geoarrow::as_geoarrow_vctr(res)), 100L)
})
