test_that("ga_cross_distance() returns one list of length(y) per row of x", {
  x <- ga_xy(c(0, 3), c(0, 4))
  y <- ga_xy(c(0, 3, 0), c(0, 4, 10))

  got <- as.vector(ga_cross_distance(x, y, "euclidean"))

  expect_length(got, 2)
  expect_equal(lengths(got), c(3L, 3L))
  expect_equal(got[[1]], c(0, 5, 10))
  expect_equal(got[[2]], c(5, 0, sqrt(3^2 + 6^2)))
})

test_that("ga_cross_distance() agrees with the pairwise form", {
  x <- ga_xy(c(-78.6382, -80.8431, -77.9447), c(35.7796, 35.2271, 34.2257))
  y <- ga_xy(c(-77.9447, -78.6382), c(34.2257, 35.7796))

  for (metric in c("haversine", "geodesic", "rhumb", "vincenty")) {
    cross <- as.vector(ga_cross_distance(x, y, metric))
    pairwise <- vapply(
      seq_len(2),
      function(j) {
        as.vector(rlang::inject(
          (!!rlang::sym(paste0("ga_dist_", metric, "_pairwise")))(
            x,
            ga_xy(c(-77.9447, -78.6382)[j], c(34.2257, 35.7796)[j])
          )
        ))
      },
      numeric(3)
    )

    expect_equal(do.call(rbind, cross), pairwise, tolerance = 1e-9)
  }
})

test_that("ga_cross_distance() measures euclidean between geometries", {
  skip_if_not_installed("sf")
  skip_if_not_installed("geoarrow")

  fp <- system.file("shape/nc.shp", package = "sf")
  nc <- as.data.frame(read_shapefile(fp))
  x <- nc$geometry[1:3]
  y <- nc$geometry[4:6]

  got <- as.vector(ga_cross_distance(x, y, "euclidean"))

  expect_equal(lengths(got), rep(3L, 3))
  expect_equal(
    vapply(got, `[`, numeric(1), 1),
    as.vector(ga_dist_euclidean_pairwise(x, nc$geometry[4]))
  )
})

test_that("ga_cross_distance() carries nulls through both sides", {
  skip_if_not_installed("geoarrow")
  skip_if_not_installed("wk")

  x <- geoarrow::as_geoarrow_array(wk::wkt(c("POINT (0 0)", NA)))
  y <- geoarrow::as_geoarrow_array(wk::wkt(c(
    "POINT (3 4)",
    NA,
    "POINT (0 10)"
  )))

  got <- as.vector(ga_cross_distance(x, y, "euclidean"))

  # a null row of x is a null element, a null row of y a null in every element
  expect_equal(got[[1]], c(5, NA, 10))
  expect_true(is.null(got[[2]]))
})

test_that("ga_cross_distance() handles an empty side", {
  x <- ga_xy(c(0, 3), c(0, 4))
  empty <- ga_xy(numeric(0), numeric(0))

  expect_equal(lengths(as.vector(ga_cross_distance(x, empty))), c(0L, 0L))
  expect_length(as.vector(ga_cross_distance(empty, x)), 0L)
})

test_that("ga_cross_distance() rejects an unknown metric", {
  x <- ga_xy(0, 0)
  expect_error(ga_cross_distance(x, x, "great-circle"), "great-circle")
})
