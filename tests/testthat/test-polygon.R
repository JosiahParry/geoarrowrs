skip_deps <- function() {
  skip_if_not_installed("sf")
  skip_if_not_installed("geoarrow")
}

as_wkt <- function(x) {
  sf::st_as_text(sf::st_as_sfc(geoarrow::as_geoarrow_vctr(x)))
}

square <- data.frame(x = c(0, 1, 1, 0), y = c(0, 0, 1, 1))

# a square with a square hole, then a triangle
shapes <- data.frame(
  x = c(0, 10, 10, 0, 2, 4, 4, 2, 20, 30, 20),
  y = c(0, 0, 10, 10, 2, 2, 4, 4, 0, 0, 10),
  p = c(rep(1, 8), 2, 2, 2),
  l = c(1, 1, 1, 1, 2, 2, 2, 2, 1, 1, 1)
)

test_that("ga_polygon closes open rings", {
  skip_deps()
  closed <- rbind(square, square[1, ])

  expect_equal(
    as_wkt(ga_polygon(square$x, square$y)),
    "POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))"
  )
  expect_equal(
    as_wkt(ga_polygon(closed$x, closed$y)),
    "POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))"
  )
  expect_equal(
    as_wkt(ga_polygon(square$x, square$y, close = FALSE)),
    "POLYGON ((0 0, 1 0, 1 1, 0 1))"
  )
})

test_that("polygon_id and linestring_id split polygons and holes", {
  skip_deps()

  expect_equal(
    as_wkt(ga_polygon(
      shapes$x,
      shapes$y,
      polygon_id = shapes$p,
      linestring_id = shapes$l
    )),
    c(
      "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0), (2 2, 4 2, 4 4, 2 4, 2 2))",
      "POLYGON ((20 0, 30 0, 20 10, 20 0))"
    )
  )

  # without linestring_id, each polygon is one ring
  expect_equal(
    as_wkt(ga_polygon(shapes$x, shapes$y, polygon_id = shapes$p)),
    c(
      "POLYGON ((0 0, 10 0, 10 10, 0 10, 2 2, 4 2, 4 4, 2 4, 0 0))",
      "POLYGON ((20 0, 30 0, 20 10, 20 0))"
    )
  )
})

test_that("a linestring_id that returns starts a new ring", {
  skip_deps()
  # without polygon_id, every ring belongs to one polygon
  expect_equal(
    as_wkt(ga_polygon(shapes$x, shapes$y, linestring_id = shapes$l)),
    paste0(
      "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0), (2 2, 4 2, 4 4, 2 4, 2 2), ",
      "(20 0, 30 0, 20 10, 20 0))"
    )
  )
})

test_that("linestring_id only has to be unique within its polygon", {
  skip_deps()
  two <- rbind(shapes[1:8, ], transform(shapes[1:8, ], x = x + 50, p = 2))

  expect_equal(
    as_wkt(ga_polygon(
      two$x,
      two$y,
      polygon_id = two$p,
      linestring_id = two$l
    )),
    c(
      "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0), (2 2, 4 2, 4 4, 2 4, 2 2))",
      "POLYGON ((50 0, 60 0, 60 10, 50 10, 50 0), (52 2, 54 2, 54 4, 52 4, 52 2))"
    )
  )
})

test_that("ga_polygon orders polygons by where each id first appears", {
  skip_deps()
  flipped <- shapes[c(9:11, 1:4), ]

  expect_equal(
    as_wkt(ga_polygon(
      flipped$x,
      flipped$y,
      polygon_id = c("b", "b", "b", rep("a", 4))
    )),
    c(
      "POLYGON ((20 0, 30 0, 20 10, 20 0))",
      "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))"
    )
  )
})

test_that("z and m add dimensions", {
  skip_deps()
  z <- c(1, 2, 3, 4)
  m <- c(5, 6, 7, 8)

  expect_equal(
    as_wkt(ga_polygon(square$x, square$y, z = z)),
    "POLYGON Z ((0 0 1, 1 0 2, 1 1 3, 0 1 4, 0 0 1))"
  )
  expect_equal(
    as_wkt(ga_polygon(square$x, square$y, m = m)),
    "POLYGON M ((0 0 5, 1 0 6, 1 1 7, 0 1 8, 0 0 5))"
  )
  expect_equal(
    as_wkt(ga_polygon(square$x, square$y, z = z, m = m)),
    "POLYGON ZM ((0 0 1 5, 1 0 2 6, 1 1 3 7, 0 1 4 8, 0 0 1 5))"
  )
})

test_that("closing compares every dimension", {
  skip_deps()
  # the last row repeats the first in x and y but not z
  expect_equal(
    as_wkt(ga_polygon(
      c(0, 1, 1, 0, 0),
      c(0, 0, 1, 1, 0),
      z = c(1, 2, 3, 9, 5)
    )),
    "POLYGON Z ((0 0 1, 1 0 2, 1 1 3, 0 1 9, 0 0 5, 0 0 1))"
  )
})

test_that("ga_polygon takes Arrow arrays, matching the plain vectors", {
  skip_deps()
  from_arrow <- ga_polygon(
    nanoarrow::as_nanoarrow_array(shapes$x),
    nanoarrow::as_nanoarrow_array(shapes$y),
    polygon_id = nanoarrow::as_nanoarrow_array(as.integer(shapes$p)),
    linestring_id = nanoarrow::as_nanoarrow_array(as.integer(shapes$l))
  )
  from_vectors <- ga_polygon(
    shapes$x,
    shapes$y,
    polygon_id = shapes$p,
    linestring_id = shapes$l
  )

  expect_equal(as_wkt(from_arrow), as_wkt(from_vectors))
})

test_that("ga_polygon returns a polygon array with no nulls", {
  skip_deps()
  res <- ga_polygon(shapes$x, shapes$y, polygon_id = shapes$p)

  expect_identical(res$length, 2L)
  expect_identical(res$null_count, 0L)
  expect_equal(as.vector(ga_geometry_type(res)), c("POLYGON", "POLYGON"))
})

test_that("ga_polygon records crs", {
  skip_deps()
  res <- ga_polygon(square$x, square$y, crs = "EPSG:4326")

  expect_equal(
    sf::st_crs(sf::st_as_sfc(geoarrow::as_geoarrow_vctr(res))),
    sf::st_crs(4326)
  )
})

test_that("ga_polygon returns an empty array for no rows", {
  skip_deps()
  expect_identical(ga_polygon(numeric(0), numeric(0))$length, 0L)
})

test_that("ga_polygon rejects a polygon_id that returns to an earlier polygon", {
  skip_deps()
  x <- c(0, 1, 1, 0, 5, 6, 6, 5, 9, 9, 8, 8)
  y <- c(0, 0, 1, 1, 5, 5, 6, 6, 9, 8, 8, 9)

  expect_error(
    ga_polygon(x, y, polygon_id = rep(c(1, 2, 1), each = 4)),
    "row 9 returns to an earlier polygon"
  )
})

test_that("ga_polygon rejects a ring too short to close", {
  skip_deps()
  expect_error(ga_polygon(c(0, 1), c(0, 1)), "at least 4")
  # close = FALSE builds the ring as given
  expect_identical(ga_polygon(c(0, 1), c(0, 1), close = FALSE)$length, 1L)
})

test_that("ga_polygon rejects NA and mismatched lengths", {
  skip_deps()
  expect_error(ga_polygon(c(0, 1, NA), c(0, 0, 1)), "`x` must not contain NA")
  expect_error(
    ga_polygon(c(0, 1, 1), c(0, 0, 1), z = c(1, NA, 1)),
    "`z` must not contain NA"
  )
  expect_error(
    ga_polygon(c(0, 1, 1), c(0, 0, 1), polygon_id = c(1, NA, 1)),
    "`polygon_id` must not contain NA"
  )
  expect_error(ga_polygon(c(0, 1, 1), c(0, 0)), "same length")
  expect_error(
    ga_polygon(c(0, 1, 1), c(0, 0, 1), linestring_id = c(1, 1)),
    "same length"
  )
})
