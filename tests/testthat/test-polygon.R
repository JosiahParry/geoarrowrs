skip_deps <- function() {
  skip_if_not_installed("sf")
  skip_if_not_installed("geoarrow")
  skip_if_not_installed("sfheaders")
}

as_wkt <- function(x) {
  sf::st_as_text(sf::st_as_sfc(geoarrow::as_geoarrow_vctr(x)))
}

sfheaders_wkt <- function(df, ...) {
  sf::st_as_text(sf::st_geometry(sfheaders::sf_polygon(df, ...)))
}

square <- data.frame(x = c(0, 1, 1, 0), y = c(0, 0, 1, 1))

# a square with a square hole, then a triangle
shapes <- data.frame(
  x = c(0, 10, 10, 0, 2, 4, 4, 2, 20, 30, 20),
  y = c(0, 0, 10, 10, 2, 2, 4, 4, 0, 0, 10),
  p = c(rep(1, 8), 2, 2, 2),
  l = c(1, 1, 1, 1, 2, 2, 2, 2, 1, 1, 1)
)

test_that("ga_polygon closes open rings, matching sfheaders", {
  skip_deps()
  closed <- rbind(square, square[1, ])

  expect_equal(
    as_wkt(ga_polygon(square$x, square$y)),
    sfheaders_wkt(square, x = "x", y = "y")
  )
  expect_equal(
    as_wkt(ga_polygon(closed$x, closed$y)),
    sfheaders_wkt(closed, x = "x", y = "y")
  )
  expect_equal(
    as_wkt(ga_polygon(square$x, square$y, close = FALSE)),
    sfheaders_wkt(square, x = "x", y = "y", close = FALSE)
  )
})

test_that("polygon_id and linestring_id split polygons and holes, matching sfheaders", {
  skip_deps()

  expect_equal(
    as_wkt(ga_polygon(
      shapes$x,
      shapes$y,
      polygon_id = shapes$p,
      linestring_id = shapes$l
    )),
    sfheaders_wkt(
      shapes,
      x = "x",
      y = "y",
      polygon_id = "p",
      linestring_id = "l"
    )
  )

  # without polygon_id, every ring belongs to one polygon
  expect_equal(
    as_wkt(ga_polygon(shapes$x, shapes$y, linestring_id = shapes$l)),
    sfheaders_wkt(shapes, x = "x", y = "y", linestring_id = "l")
  )

  # without linestring_id, each polygon is one ring
  expect_equal(
    as_wkt(ga_polygon(shapes$x, shapes$y, polygon_id = shapes$p)),
    sfheaders_wkt(shapes, x = "x", y = "y", polygon_id = "p")
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
    sfheaders_wkt(two, x = "x", y = "y", polygon_id = "p", linestring_id = "l")
  )
})

test_that("ga_polygon orders polygons by where each id first appears", {
  skip_deps()
  flipped <- transform(
    shapes[c(9:11, 1:4), ],
    p = c("b", "b", "b", rep("a", 4))
  )

  expect_equal(
    as_wkt(ga_polygon(flipped$x, flipped$y, polygon_id = flipped$p)),
    sfheaders_wkt(flipped, x = "x", y = "y", polygon_id = "p")
  )
})

test_that("z and m add dimensions, matching sfheaders", {
  skip_deps()
  zm <- transform(square, z = c(1, 2, 3, 4), m = c(5, 6, 7, 8))

  expect_equal(
    as_wkt(ga_polygon(zm$x, zm$y, z = zm$z)),
    sfheaders_wkt(zm, x = "x", y = "y", z = "z")
  )
  expect_equal(
    as_wkt(ga_polygon(zm$x, zm$y, m = zm$m)),
    sfheaders_wkt(zm, x = "x", y = "y", m = "m")
  )
  expect_equal(
    as_wkt(ga_polygon(zm$x, zm$y, z = zm$z, m = zm$m)),
    sfheaders_wkt(zm, x = "x", y = "y", z = "z", m = "m")
  )
})

test_that("closing compares every dimension, matching sfheaders", {
  skip_deps()
  # the last row repeats the first in x and y but not z
  ring <- data.frame(
    x = c(0, 1, 1, 0, 0),
    y = c(0, 0, 1, 1, 0),
    z = c(1, 2, 3, 9, 5)
  )

  expect_equal(
    as_wkt(ga_polygon(ring$x, ring$y, z = ring$z)),
    sfheaders_wkt(ring, x = "x", y = "y", z = "z")
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

test_that("a linestring_id that returns starts a new ring, matching sfheaders", {
  skip_deps()
  again <- transform(shapes, p = 1, l = rep(c(1, 2, 1), c(4, 4, 3)))

  expect_equal(
    as_wkt(ga_polygon(
      again$x,
      again$y,
      polygon_id = again$p,
      linestring_id = again$l
    )),
    sfheaders_wkt(
      again,
      x = "x",
      y = "y",
      polygon_id = "p",
      linestring_id = "l"
    )
  )
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
