skip_deps <- function() {
  skip_if_not_installed("sf")
  skip_if_not_installed("geoarrow")
}

as_wkt <- function(x) {
  sf::st_as_text(sf::st_as_sfc(geoarrow::as_geoarrow_vctr(x)))
}

sf_line_wkt <- function(...) sf::st_as_text(sf::st_linestring(cbind(...)))

test_that("ga_make_line_agg joins every point into one line, matching sf", {
  skip_deps()
  x <- c(0, 1, 2, 10, 11)
  y <- c(0, 1, 0, 5, 5)

  expect_equal(as_wkt(ga_make_line_agg(ga_xy(x, y))), sf_line_wkt(x, y))
})

test_that("by groups an unsorted key by first appearance, matching sf per group", {
  skip_deps()
  nc <- as.data.frame(read_shapefile(system.file(
    "shape/nc.shp",
    package = "sf"
  )))
  centres <- ga_centroid(nc$geometry)
  x <- as.vector(ga_x(centres))
  y <- as.vector(ga_y(centres))
  key <- substr(nc$NAME, 1, 1)

  got <- as_wkt(ga_make_line_agg(centres, by = key))
  want <- vapply(
    unique(key),
    function(l) {
      if (sum(key == l) < 2) {
        return(NA_character_)
      }
      sf_line_wkt(x[key == l], y[key == l])
    },
    character(1)
  )

  lines <- !is.na(want)
  expect_equal(got[lines], unname(want[lines]))
})

test_that("a group with fewer than two points is a null line", {
  skip_deps()
  pts <- ga_xy(c(0, 1, 5), c(0, 1, 5))
  res <- ga_make_line_agg(pts, by = c("a", "a", "b"))

  expect_identical(res$length, 2L)
  expect_identical(res$null_count, 1L)
  expect_equal(as.vector(ga_geometry_type(res)), c("LINESTRING", NA))
})

test_that("ga_make_line_agg skips null and empty points and keeps z", {
  skip_deps()
  pts <- geoarrow::as_geoarrow_array(
    wk::wkt(c(
      "POINT Z (0 0 1)",
      "POINT Z (1 1 2)",
      NA,
      "POINT Z EMPTY",
      "POINT Z (2 0 3)"
    )),
    schema = geoarrow::geoarrow_point(dimensions = "XYZ")
  )

  expect_equal(
    as_wkt(ga_make_line_agg(pts)),
    sf_line_wkt(c(0, 1, 2), c(0, 1, 0), c(1, 2, 3))
  )
})

test_that("ga_make_line_agg keeps m", {
  skip_deps()
  pts <- geoarrow::as_geoarrow_array(
    wk::wkt(c("POINT ZM (0 0 1 9)", "POINT ZM (1 1 2 8)")),
    schema = geoarrow::geoarrow_point(dimensions = "XYZM")
  )

  expect_equal(
    as_wkt(ga_make_line_agg(pts)),
    "LINESTRING ZM (0 0 1 9, 1 1 2 8)"
  )
})

test_that("a numeric or Arrow by matches the same labels as text", {
  skip_deps()
  pts <- ga_xy(c(0, 1, 2, 3, 4), c(0, 1, 0, 1, 0))
  key <- c(2L, 1L, 2L, 1L, 2L)
  from_text <- as_wkt(ga_make_line_agg(pts, by = as.character(key)))

  expect_equal(as_wkt(ga_make_line_agg(pts, by = key)), from_text)
  expect_equal(
    as_wkt(ga_make_line_agg(pts, by = nanoarrow::as_nanoarrow_array(key))),
    from_text
  )
})

test_that("ga_make_line_agg keeps crs", {
  skip_deps()
  res <- ga_make_line_agg(ga_xy(c(0, 1), c(0, 1), crs = "EPSG:4326"))

  expect_equal(
    sf::st_crs(sf::st_as_sfc(geoarrow::as_geoarrow_vctr(res))),
    sf::st_crs(4326)
  )
})

test_that("by has to have one label per point", {
  skip_deps()
  expect_error(
    ga_make_line_agg(ga_xy(c(0, 1, 2), c(0, 1, 0)), by = 1:2),
    "one label per row of `points`"
  )
})
