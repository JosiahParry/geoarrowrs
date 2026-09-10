skip_arrow <- function() {
  skip_if_not_installed("arrow")
  skip_if_not_installed("dplyr")
  skip_if_not_installed("geoarrow")
  skip_if_not_installed("sf")
  skip_if_not(
    arrow::arrow_info()$capabilities[["acero"]],
    "acero not available"
  )
}

nc_table <- function() {
  skip_arrow()
  requireNamespace("geoarrow", quietly = TRUE)
  path <- system.file("shape/nc.shp", package = "sf")
  skip_if(path == "")
  arrow::arrow_table(as.data.frame(read_shapefile(path)))
}

test_that("register_geoarrow_udfs registers most of the catalogue", {
  tbl <- nc_table()
  registered <- suppressWarnings(register_geoarrow_udfs(tbl, prefix = "t1_"))

  expect_gt(length(registered), 30L)
  expect_true("t1_unsigned_area" %in% registered)
  expect_true("t1_intersects" %in% registered)
})

test_that("a unary function runs inside mutate on a Table", {
  tbl <- nc_table()
  suppressWarnings(register_geoarrow_udfs(tbl, prefix = "t2_"))

  res <- dplyr::collect(dplyr::select(
    dplyr::mutate(tbl, a = t2_unsigned_area(geometry)),
    a
  ))

  expect_equal(nrow(res), 100L)
  expect_type(res$a, "double")
})

test_that("the engine result matches calling the function directly", {
  tbl <- nc_table()
  suppressWarnings(register_geoarrow_udfs(tbl, prefix = "t3_"))

  via_arrow <- dplyr::collect(dplyr::select(
    dplyr::mutate(tbl, a = t3_unsigned_area(geometry)),
    a
  ))$a
  direct <- as.vector(nanoarrow::convert_array(
    unsigned_area(as.data.frame(tbl)$geometry)
  ))

  expect_equal(via_arrow, direct)
})

test_that("a geometry returning function keeps its geoarrow type", {
  tbl <- nc_table()
  suppressWarnings(register_geoarrow_udfs(tbl, prefix = "t4_"))

  res <- dplyr::collect(dplyr::select(
    dplyr::mutate(tbl, cen = t4_centroid(geometry)),
    cen
  ))

  expect_s3_class(res$cen, "geoarrow_vctr")
  expect_equal(nrow(res), 100L)
})

test_that("a binary predicate runs inside mutate", {
  tbl <- nc_table()
  suppressWarnings(register_geoarrow_udfs(tbl, prefix = "t5_"))

  res <- dplyr::collect(dplyr::select(
    dplyr::mutate(tbl, s = t5_intersects(geometry, geometry)),
    s
  ))

  expect_true(all(res$s))
})

test_that("a registered function can be filtered on", {
  tbl <- nc_table()
  suppressWarnings(register_geoarrow_udfs(tbl, prefix = "t6_"))

  kept <- dplyr::collect(dplyr::summarise(
    dplyr::filter(tbl, t6_unsigned_area(geometry) > 0.1),
    n = dplyr::n()
  ))$n
  direct <- sum(
    as.vector(nanoarrow::convert_array(
      unsigned_area(as.data.frame(tbl)$geometry)
    )) >
      0.1
  )

  expect_equal(kept, direct)
})

test_that("registration works against an on disk dataset", {
  tbl <- nc_table()
  suppressWarnings(register_geoarrow_udfs(tbl, prefix = "t7_"))

  dir <- file.path(tempdir(), "geoarrowrs-udf-ds")
  on.exit(unlink(dir, recursive = TRUE), add = TRUE)
  unlink(dir, recursive = TRUE)
  arrow::write_dataset(tbl, dir, format = "feather")

  res <- dplyr::collect(dplyr::select(
    dplyr::mutate(
      arrow::open_dataset(dir, format = "feather"),
      a = t7_unsigned_area(geometry)
    ),
    a
  ))

  expect_equal(nrow(res), 100L)
})

test_that("functions that do not apply to the type are reported not registered", {
  tbl <- nc_table()

  expect_warning(
    registered <- register_geoarrow_udfs(tbl, prefix = "t8_"),
    "Could not register"
  )
  expect_false("t8_length_euclidean" %in% registered)
})

test_that("a subset of functions can be registered", {
  tbl <- nc_table()
  registered <- register_geoarrow_udfs(
    tbl,
    functions = c("centroid", "unsigned_area"),
    prefix = "t9_"
  )

  expect_setequal(registered, c("t9_centroid", "t9_unsigned_area"))
})

test_that("an unregisterable name is rejected", {
  tbl <- nc_table()

  expect_error(
    register_geoarrow_udfs(tbl, functions = "buffer"),
    "Only functions whose arguments are all geometries"
  )
})

test_that("a missing column is reported", {
  tbl <- nc_table()
  expect_error(register_geoarrow_udfs(tbl, column = "nope"), "not found")
})
