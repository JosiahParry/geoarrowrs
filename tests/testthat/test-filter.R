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

test_that("ga_filter keeps the rows that matched", {
  res <- ga_filter(sites, counties, ga_sparse_within)
  expect_s3_class(res, "Table")

  res <- as.data.frame(res)
  expect_identical(res$site, c("a", "b"))
  expect_identical(names(res), c("site", "geometry"))
})

test_that("ga_filter does not repeat a row that matched several", {
  # every county touches at least one other, so all of them survive, once each
  res <- ga_filter(counties, counties, ga_sparse_touches)
  expect_identical(nrow(res), nrow(counties))
})

test_that("ga_filter agrees with the join it is the short form of", {
  joined <- as.data.frame(
    ga_join(sites, counties, ga_sparse_within, left = FALSE)
  )
  filtered <- as.data.frame(ga_filter(sites, counties, ga_sparse_within))
  expect_identical(filtered$site, unique(joined$site))
})

test_that("the default predicate is intersects", {
  default <- as.data.frame(ga_filter(sites, counties))
  explicit <- as.data.frame(ga_filter(sites, counties, ga_sparse_intersects))
  expect_equal(default$site, explicit$site)
})

test_that("ga_filter rejects a frame with no geometry column", {
  expect_error(
    ga_filter(data.frame(a = 1), counties),
    "no GeoArrow geometry column"
  )
  expect_error(ga_filter(sites, counties, "within"), "must be a function")
})
