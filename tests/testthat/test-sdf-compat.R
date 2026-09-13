skip_if_not_installed("sdf")
skip_if_not_installed("geoarrow")
skip_if_not_installed("sf")

nc <- as.data.frame(read_shapefile(
  system.file("shape/nc.shp", package = "sf")
))
geom <- geoarrow::as_geoarrow_vctr(nc$geometry)
sites <- geoarrow::as_geoarrow_vctr(ga_xy(c(-78.6, -80.8), c(35.8, 35.2)))

counties <- tibble::tibble(
  name = as.character(nc$NAME),
  geometry = geom
)

test_that("a geoarrow geometry column makes a spatial data frame", {
  s <- sdf::as_sdf(counties)
  expect_s3_class(s, "sdf")
  expect_identical(nrow(s), 100L)
})

test_that("the required generics are implemented", {
  expect_true(sdf::is_geometry(geom))
  expect_s3_class(sdf::combine_geometry(geom), "geoarrow_vctr")
  expect_identical(length(sdf::combine_geometry(geom)), 1L)
})

test_that("bounding_box agrees with wk, which is sdf's default", {
  skip_if_not_installed("wk")
  expect_equal(sdf::bounding_box(geom), unlist(wk::wk_bbox(geom)))
})

test_that("the predicates return a plain list of row positions", {
  # sdf_join() calls lengths() and [[ on this, which a list array does not answer
  hits <- sdf::sdf_intersects(geom[1:4], geom[1:12])

  expect_type(hits, "list")
  expect_type(hits[[1]], "integer")
  expect_length(hits, 4L)
  expect_identical(
    hits,
    lapply(as.vector(ga_sparse_intersects(geom[1:4], geom[1:12])), as.integer)
  )
})

test_that("the predicates agree with the sparse predicates they wrap", {
  expect_identical(
    sdf::sdf_within(sites, geom),
    lapply(as.vector(ga_sparse_within(sites, geom)), as.integer)
  )
  expect_identical(
    sdf::sdf_touches(geom[1:5], geom[1:12]),
    lapply(as.vector(ga_sparse_touches(geom[1:5], geom[1:12])), as.integer)
  )
})

test_that("sdf_disjoint is the complement of sdf_intersects", {
  hits <- sdf::sdf_intersects(geom[1:4], geom[1:12])
  miss <- sdf::sdf_disjoint(geom[1:4], geom[1:12])

  for (i in seq_len(4)) {
    expect_identical(sort(c(hits[[i]], miss[[i]])), seq_len(12L))
  }
})

test_that("sdf_filter() keeps the rows that relate", {
  kept <- sdf::sdf_filter(
    sdf::as_sdf(counties),
    sdf::as_sdf(tibble::tibble(
      id = 1:2,
      geometry = sites
    ))
  )

  expect_identical(nrow(kept), 2L)
  expect_setequal(kept$name, c("Wake", "Mecklenburg"))
})

test_that("the optional generics return the GeoArrow representation", {
  expect_s3_class(sdf::centroid(geom), "geoarrow_vctr")
  expect_s3_class(sdf::convex_hull(geom), "geoarrow_vctr")
  expect_s3_class(sdf::union_geometry(geom), "geoarrow_vctr")
  expect_s3_class(sdf::buffer_geometry(geom[1:3], 0.1), "geoarrow_vctr")
  expect_s3_class(sdf::simplify_geometry(geom[1:3], 0.05), "geoarrow_vctr")

  expect_identical(sdf::sdf_area(geom), as.vector(ga_unsigned_area(geom)))
})
