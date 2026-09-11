test_that("a spherical metric measures metres between points", {
  # Raleigh to Charlotte is about 2.3 degrees, and about 209km
  raleigh <- ga_xy(-78.6382, 35.7796)
  charlotte <- ga_xy(-80.8431, 35.2271)

  planar <- as.vector(ga_sparse_dwithin(raleigh, charlotte, 3))
  expect_equal(planar[[1]], 1L)
  expect_length(as.vector(ga_sparse_dwithin(raleigh, charlotte, 2))[[1]], 0L)

  metres <- as.vector(
    ga_sparse_dwithin(raleigh, charlotte, 250000, metric = "geodesic")
  )
  expect_equal(metres[[1]], 1L)
  expect_length(
    as.vector(
      ga_sparse_dwithin(raleigh, charlotte, 200000, metric = "geodesic")
    )[[1]],
    0L
  )
})

test_that("the dwithin radius agrees with the pairwise distance", {
  x <- ga_xy(c(-78.6382, 0), c(35.7796, 0))
  y <- ga_xy(c(-80.8431, 10), c(35.2271, 60))

  for (metric in c("haversine", "geodesic", "rhumb")) {
    exact <- as.vector(rlang::inject(
      (!!rlang::sym(paste0("ga_dist_", metric, "_pairwise")))(x, y)
    ))

    # a radius just over each exact distance matches, just under does not
    for (i in seq_along(exact)) {
      xi <- ga_xy(as.vector(ga_x(x))[i], as.vector(ga_y(x))[i])
      yi <- ga_xy(as.vector(ga_x(y))[i], as.vector(ga_y(y))[i])
      over <- as.vector(ga_sparse_dwithin(xi, yi, exact[i] * 1.001, metric))
      under <- as.vector(ga_sparse_dwithin(xi, yi, exact[i] * 0.999, metric))
      expect_equal(over[[1]], 1L)
      expect_length(under[[1]], 0L)
    }
  }
})

test_that("knn ranks by the metric it was given", {
  # at latitude 60 a degree of longitude is half a degree of latitude on the
  # ground, so 2 degrees north is the nearer in degrees and the further in
  # metres than 3 degrees east: 222km against 167km
  from <- ga_xy(0, 60)
  candidates <- ga_xy(c(0, 3), c(62, 60))

  planar <- as.vector(ga_sparse_knn(from, candidates, k = 1))[[1]]
  expect_equal(planar$row, 1L)

  geodesic <- as.vector(
    ga_sparse_knn(from, candidates, k = 1, metric = "geodesic")
  )[[1]]
  expect_equal(geodesic$row, 2L)
})

test_that("knn distances agree with the pairwise function", {
  x <- ga_xy(-78.6382, 35.7796)
  y <- ga_xy(c(-80.8431, -77.9447), c(35.2271, 34.2257))

  for (metric in c("haversine", "geodesic", "rhumb")) {
    got <- as.vector(ga_sparse_knn(x, y, k = 2, metric = metric))[[1]]
    want <- as.vector(rlang::inject(
      (!!rlang::sym(paste0("ga_dist_", metric, "_pairwise")))(
        ga_xy(rep(-78.6382, 2), rep(35.7796, 2)),
        y
      )
    ))
    expect_equal(got$distance, sort(want), tolerance = 1e-9)
  }
})

test_that("a spherical metric refuses anything but points", {
  skip_if_not_installed("sf")
  skip_if_not_installed("geoarrow")

  nc <- as.data.frame(read_shapefile(
    system.file("shape/nc.shp", package = "sf")
  ))
  sites <- ga_xy(c(-78.6, -80.8), c(35.8, 35.2))

  expect_error(
    ga_sparse_knn(sites, nc$geometry, metric = "geodesic"),
    "measures between points"
  )
  expect_error(
    ga_sparse_dwithin(sites, nc$geometry, 1000, metric = "haversine"),
    "measures between points"
  )

  # euclidean is defined for every pair, so the same call is fine
  expect_length(as.vector(ga_sparse_knn(sites, nc$geometry)), 2L)
})

test_that("the metric is matched against the ones that exist", {
  x <- ga_xy(0, 0)
  expect_error(ga_sparse_knn(x, x, metric = "great-circle"), "euclidean")
  expect_error(ga_sparse_dwithin(x, x, 1, metric = "vincenty"), "euclidean")
})

test_that("euclidean is the default, and it is what it always was", {
  x <- ga_xy(c(0, 5), c(0, 5))
  y <- ga_xy(c(1, 2), c(1, 2))

  expect_equal(
    as.vector(ga_sparse_dwithin(x, y, 2)),
    as.vector(ga_sparse_dwithin(x, y, 2, metric = "euclidean"))
  )
  expect_equal(
    as.vector(ga_sparse_knn(x, y, k = 2)),
    as.vector(ga_sparse_knn(x, y, k = 2, metric = "euclidean"))
  )
})
