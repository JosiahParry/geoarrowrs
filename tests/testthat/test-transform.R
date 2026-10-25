test_that("ga_transform is a placeholder that errors", {
  pts <- ga_point(0, 0, crs = "EPSG:4326")
  expect_error(ga_transform(pts, "EPSG:27572"), "not implemented yet")
  expect_false("ga_transform" %in% getNamespaceExports("geoarrowrs"))
})
