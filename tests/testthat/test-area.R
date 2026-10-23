skip_deps <- function() {
  skip_if_not_installed("sf")
  skip_if_not_installed("geoarrow")
}

nc <- function() {
  skip_deps()
  as.data.frame(
    read_shapefile(system.file("shape/nc.shp", package = "sf"))
  )$geometry
}

test_that("signed area is negative for a clockwise ring", {
  g <- nc()

  expect_true(all(as.vector(ga_signed_area(g)) < 0))
  expect_equal(
    as.vector(ga_unsigned_area(g)),
    abs(as.vector(ga_signed_area(g)))
  )
})

test_that("unsigned geodesic area needs counter clockwise winding", {
  g <- nc()

  # geo assumes Simple Features winding, so a clockwise ring names the rest
  # of the earth instead of the county
  expect_true(all(as.vector(ga_unsigned_area_geodesic(g)) > 5e14))

  ccw <- ga_orient(g, "default")
  expect_equal(
    as.vector(ga_unsigned_area_geodesic(ccw)),
    abs(as.vector(ga_signed_area_geodesic(g))),
    tolerance = 1e-6
  )
})

test_that("signed geodesic area is unaffected by winding", {
  g <- nc()
  ccw <- ga_orient(g, "default")

  expect_equal(
    as.vector(ga_signed_area_geodesic(g)),
    -as.vector(ga_signed_area_geodesic(ccw)),
    tolerance = 1e-6
  )
})

test_that("geodesic perimeter is a single function", {
  g <- nc()
  res <- as.vector(ga_perimeter_geodesic(g))

  expect_length(res, 100L)
  expect_true(all(res > 0))
})

test_that("euclidean perimeter matches sf", {
  skip_deps()
  skip_if_not_installed("lwgeom")
  sq <- rbind(c(0, 0), c(2, 0), c(2, 2), c(0, 2), c(0, 0))
  hole <- rbind(c(0.5, 0.5), c(0.5, 1), c(1, 1), c(1, 0.5), c(0.5, 0.5))
  x <- sf::st_sfc(
    sf::st_point(c(0, 0)),
    sf::st_linestring(rbind(c(0, 0), c(3, 4))),
    sf::st_polygon(list(sq)),
    sf::st_polygon(list(sq, hole)),
    sf::st_multipolygon(list(list(sq), list(sq + 3, hole + 3))),
    sf::st_polygon()
  )

  expect_equal(
    as.vector(ga_perimeter_euclidean(geoarrow::as_geoarrow_array(x))),
    sf::st_perimeter(x)
  )
})

test_that("euclidean perimeter matches sf on nc", {
  skip_if_not_installed("lwgeom")
  g <- nc()
  x <- sf::st_set_crs(
    sf::st_read(
      system.file("shape/nc.shp", package = "sf"),
      quiet = TRUE
    ),
    NA
  )

  expect_equal(
    as.vector(ga_perimeter_euclidean(g)),
    as.numeric(sf::st_perimeter(x))
  )
})

test_that("area is length preserving and null propagating", {
  skip_deps()
  g <- geoarrow::as_geoarrow_array(sf::st_sfc(
    sf::st_polygon(list(rbind(c(0, 0), c(2, 0), c(2, 2), c(0, 2), c(0, 0)))),
    sf::st_polygon()
  ))

  res <- ga_unsigned_area(g)
  expect_equal(res$length, 2L)
  expect_equal(as.vector(res)[1], 4)
})
