skip_if_not_installed("sf")
skip_if_not_installed("geoarrow")

nc <- as.data.frame(read_shapefile(
  system.file("shape/nc.shp", package = "sf")
))
counties <- nc[c("NAME", "geometry")]

sites <- data.frame(
  site = c("a", "b", "z"),
  geometry = geoarrow::as_geoarrow_vctr(
    ga_xy(c(-78.6, -80.8, 0), c(35.8, 35.2, 0))
  )
)

test_that("points join to the county holding them", {
  res <- ga_join(sites, counties, ga_sparse_within)
  expect_identical(res$site, c("a", "b", "z"))
  expect_identical(res$NAME, c("Wake", "Mecklenburg", NA_character_))
})

test_that("left = FALSE drops rows that match nothing", {
  res <- ga_join(sites, counties, ga_sparse_within, left = FALSE)
  expect_identical(res$site, c("a", "b"))
})

test_that("a row matching several rows is repeated", {
  res <- ga_join(counties, counties, ga_sparse_touches)
  n <- lengths(as.vector(ga_sparse_touches(nc$geometry, nc$geometry)))
  expect_identical(nrow(res), sum(n))
  expect_identical(res$NAME_x, rep.int(counties$NAME, n))
})

test_that("shared names are suffixed and geometry comes from x", {
  res <- ga_join(counties, counties, ga_sparse_touches)
  expect_identical(names(res), c("NAME_x", "geometry", "NAME_y"))
  expect_s3_class(res$geometry, "geoarrow_vctr")

  res <- ga_join(counties, counties, ga_sparse_touches, suffix = c("_l", "_r"))
  expect_identical(names(res), c("NAME_l", "geometry", "NAME_r"))
})

test_that("the default predicate is intersects", {
  expect_identical(
    ga_join(sites, counties),
    ga_join(sites, counties, ga_sparse_intersects)
  )
})

test_that("a frame with no geometry column is an error", {
  expect_error(
    ga_join(data.frame(a = 1), counties),
    "no GeoArrow geometry column"
  )
  expect_error(
    ga_join(counties, data.frame(a = 1)),
    "no GeoArrow geometry column"
  )
})

test_that("a frame with two geometry columns is an error", {
  two <- counties
  two$other <- counties$geometry
  expect_error(ga_join(two, counties), "more than one GeoArrow geometry column")
})

test_that("bad arguments are caught", {
  expect_error(ga_join(sites, counties, "intersects"), "must be a function")
  expect_error(ga_join(sites, counties, suffix = ".x"), "length 2")
  expect_error(ga_join(sites, counties, TRUE), "must be a function")
})
