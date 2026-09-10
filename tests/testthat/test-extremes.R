skip_if_no_sf <- function() {
  skip_if_not_installed("sf")
  skip_if_not_installed("nanoarrow")
}

as_geoarrow <- function(sfc) {
  skip_if_not_installed("geoarrow")
  geoarrow::as_geoarrow_array(sfc)
}

# nanoarrow leaves the geoarrow.point children as extension arrays rather than
# converting them, so reach into the x/y buffers directly.
pt <- function(res, name) {
  a <- res$children[[name]]
  list(
    x = as.vector(a$children$x),
    y = as.vector(a$children$y)
  )
}

test_that("extremes returns four point fields, one row per geometry", {
  skip_if_no_sf()

  # An L-shaped triangle so each extreme is a distinct vertex.
  g <- sf::st_sfc(sf::st_polygon(list(
    matrix(c(0, 0, 4, 1, 2, 5, 0, 0), ncol = 2, byrow = TRUE)
  )))

  res <- ga_extremes(as_geoarrow(g))
  schema <- nanoarrow::infer_nanoarrow_schema(res)

  expect_equal(names(schema$children), c("x_min", "x_max", "y_min", "y_max"))
  expect_equal(length(pt(res, "x_min")$x), 1L)
})

test_that("extremes finds the correct vertices", {
  skip_if_no_sf()

  g <- sf::st_sfc(sf::st_polygon(list(
    matrix(c(0, 0, 4, 1, 2, 5, 0, 0), ncol = 2, byrow = TRUE)
  )))

  res <- ga_extremes(as_geoarrow(g))

  # These are extreme vertices, not bounding box corners: x_max is (4, 1),
  # carrying the y of the rightmost vertex rather than the bbox's y_max of 5.
  expect_equal(unlist(pt(res, "x_min"), use.names = FALSE), c(0, 0))
  expect_equal(unlist(pt(res, "x_max"), use.names = FALSE), c(4, 1))
  expect_equal(unlist(pt(res, "y_min"), use.names = FALSE), c(0, 0))
  expect_equal(unlist(pt(res, "y_max"), use.names = FALSE), c(2, 5))
})

test_that("extremes works on multipolygon arrays, not just mixed geometry", {
  skip_if_no_sf()

  g <- sf::st_sfc(sf::st_multipolygon(list(
    list(matrix(c(0, 0, 1, 0, 1, 1, 0, 1, 0, 0), ncol = 2, byrow = TRUE)),
    list(matrix(c(5, 5, 7, 5, 7, 7, 5, 7, 5, 5), ncol = 2, byrow = TRUE))
  )))

  res <- ga_extremes(as_geoarrow(g))

  expect_equal(length(pt(res, "x_min")$x), 1L)
  expect_equal(pt(res, "x_min")$x, 0)
  expect_equal(pt(res, "x_max")$x, 7)
})

test_that("extremes is vectorized over many geometries", {
  skip_if_no_sf()

  g <- sf::st_sfc(
    sf::st_polygon(list(matrix(
      c(0, 0, 1, 0, 1, 1, 0, 0),
      ncol = 2,
      byrow = TRUE
    ))),
    sf::st_polygon(list(matrix(
      c(10, 10, 12, 10, 12, 12, 10, 10),
      ncol = 2,
      byrow = TRUE
    )))
  )

  res <- ga_extremes(as_geoarrow(g))

  expect_equal(length(pt(res, "x_max")$x), 2L)
  expect_equal(pt(res, "x_max")$x, c(1, 12))
})

test_that("an empty geometry yields a null row", {
  skip_if_no_sf()

  g <- sf::st_sfc(
    sf::st_polygon(list(matrix(
      c(0, 0, 1, 0, 1, 1, 0, 0),
      ncol = 2,
      byrow = TRUE
    ))),
    sf::st_polygon()
  )

  res <- ga_extremes(as_geoarrow(g))
  xs <- pt(res, "x_min")$x

  expect_equal(length(xs), 2L)
  expect_equal(xs[1], 0)
  expect_true(is.na(xs[2]))
})
