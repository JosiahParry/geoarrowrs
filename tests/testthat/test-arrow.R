skip_arrow <- function() {
  skip_if_not_installed("arrow")
  skip_if_not_installed("dplyr")
  skip_if_not_installed("geoarrow")
  skip_if_not_installed("nanoarrow")
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

vctr_table <- function(sfc, ...) {
  skip_arrow()
  requireNamespace("geoarrow", quietly = TRUE)
  arrow::arrow_table(
    id = seq_along(sfc),
    geometry = geoarrow::as_geoarrow_vctr(geoarrow::as_geoarrow_array(sfc)),
    ...
  )
}

test_that("the whole catalogue registers without warning", {
  tbl <- nc_table()

  expect_no_warning(register_geoarrow_udfs(crs = tbl, prefix = "t1_"))
  registered <- register_geoarrow_udfs(crs = tbl, prefix = "t1_")

  expect_equal(length(registered), length(names(geoarrow_udf_catalogue())))
  expect_true("t1_ga_unsigned_area" %in% registered)
  expect_true("t1_ga_length_euclidean" %in% registered)
  expect_true("t1_ga_simplify" %in% registered)
})

test_that("a unary function runs inside mutate on a Table", {
  tbl <- nc_table()
  register_geoarrow_udfs(
    crs = tbl,
    functions = "ga_unsigned_area",
    prefix = "t2_"
  )

  res <- dplyr::collect(dplyr::select(
    dplyr::mutate(tbl, a = t2_ga_unsigned_area(geometry)),
    a
  ))
  direct <- as.vector(nanoarrow::convert_array(
    ga_unsigned_area(as.data.frame(tbl)$geometry)
  ))

  expect_equal(nrow(res), 100L)
  expect_equal(res$a, direct)
})

test_that("a geometry returning function keeps its geoarrow type and CRS", {
  tbl <- nc_table()
  register_geoarrow_udfs(crs = tbl, functions = "ga_centroid", prefix = "t3_")

  res <- dplyr::collect(dplyr::select(
    dplyr::mutate(tbl, cen = t3_ga_centroid(geometry)),
    cen
  ))

  expect_s3_class(res$cen, "geoarrow_vctr")
  expect_equal(nrow(res), 100L)
  expect_equal(
    wk::wk_crs(res$cen),
    wk::wk_crs(as.data.frame(tbl)$geometry)
  )
})

test_that("a binary predicate pairs two geometry columns", {
  tbl <- nc_table()
  register_geoarrow_udfs(crs = tbl, functions = "ga_intersects", prefix = "t4_")

  res <- dplyr::collect(dplyr::select(
    dplyr::mutate(tbl, s = t4_ga_intersects(geometry, geometry)),
    s
  ))

  expect_true(all(res$s))
})

test_that("a binary predicate pairs two different geometry types", {
  tbl <- nc_table()
  geometry <- as.data.frame(tbl)$geometry
  pt <- geoarrow::as_geoarrow_vctr(ga_centroid(geometry))
  both <- arrow::arrow_table(geometry = geometry, pt = pt)
  register_geoarrow_udfs(crs = tbl, functions = "ga_contains", prefix = "t5_")

  res <- dplyr::collect(dplyr::select(
    dplyr::mutate(both, hit = t5_ga_contains(geometry, pt)),
    hit
  ))

  expect_equal(nrow(res), 100L)
  expect_type(res$hit, "logical")
})

test_that("a numeric argument can be a literal", {
  tbl <- nc_table()
  register_geoarrow_udfs(crs = tbl, functions = "ga_simplify", prefix = "t6_")

  res <- dplyr::collect(dplyr::select(
    dplyr::mutate(tbl, s = t6_ga_simplify(geometry, 0.05)),
    s
  ))
  direct <- geoarrow::as_geoarrow_vctr(
    ga_simplify(as.data.frame(tbl)$geometry, 0.05)
  )

  expect_equal(as.character(res$s), as.character(direct))
})

test_that("a numeric argument can be a column", {
  tbl <- nc_table()
  with_eps <- arrow::arrow_table(
    geometry = as.data.frame(tbl)$geometry,
    eps = rep(0.05, 100)
  )
  register_geoarrow_udfs(crs = tbl, functions = "ga_simplify", prefix = "t7_")

  res <- dplyr::collect(dplyr::select(
    dplyr::mutate(with_eps, s = t7_ga_simplify(geometry, eps)),
    s
  ))
  direct <- geoarrow::as_geoarrow_vctr(
    ga_simplify(as.data.frame(tbl)$geometry, 0.05)
  )

  expect_equal(as.character(res$s), as.character(direct))
})

test_that("a string option argument is passed through", {
  tbl <- nc_table()
  register_geoarrow_udfs(crs = tbl, functions = "ga_densify", prefix = "t8_")

  res <- dplyr::collect(dplyr::select(
    dplyr::mutate(tbl, d = t8_ga_densify(geometry, 0.5, "haversine")),
    d
  ))
  direct <- geoarrow::as_geoarrow_vctr(
    ga_densify(as.data.frame(tbl)$geometry, 0.5, "haversine")
  )

  expect_equal(as.character(res$d), as.character(direct))
})

test_that("an integer option argument is passed through", {
  tbl <- nc_table()
  register_geoarrow_udfs(
    crs = tbl,
    functions = "ga_chaikin_smoothing",
    prefix = "t9_"
  )

  res <- dplyr::collect(dplyr::select(
    dplyr::mutate(tbl, k = t9_ga_chaikin_smoothing(geometry, 2L)),
    k
  ))
  direct <- geoarrow::as_geoarrow_vctr(
    ga_chaikin_smoothing(as.data.frame(tbl)$geometry, 2L)
  )

  expect_equal(as.character(res$k), as.character(direct))
})

test_that("a registered function can be filtered on", {
  tbl <- nc_table()
  register_geoarrow_udfs(
    crs = tbl,
    functions = "ga_unsigned_area",
    prefix = "t10_"
  )

  kept <- dplyr::collect(dplyr::summarise(
    dplyr::filter(tbl, t10_ga_unsigned_area(geometry) > 0.1),
    n = dplyr::n()
  ))$n
  direct <- sum(
    as.vector(nanoarrow::convert_array(
      ga_unsigned_area(as.data.frame(tbl)$geometry)
    )) >
      0.1
  )

  expect_equal(kept, direct)
})

test_that("registration works against an on disk dataset", {
  tbl <- nc_table()
  register_geoarrow_udfs(
    crs = tbl,
    functions = "ga_unsigned_area",
    prefix = "t11_"
  )

  dir <- file.path(tempdir(), "geoarrowrs-udf-ds")
  on.exit(unlink(dir, recursive = TRUE), add = TRUE)
  unlink(dir, recursive = TRUE)
  arrow::write_dataset(tbl, dir, format = "feather")

  res <- dplyr::collect(dplyr::select(
    dplyr::mutate(
      arrow::open_dataset(dir, format = "feather"),
      a = t11_ga_unsigned_area(geometry)
    ),
    a
  ))

  expect_equal(nrow(res), 100L)
})

test_that("one call covers every geometry type", {
  lines <- vctr_table(sf::st_sfc(sf::st_linestring(cbind(c(0, 3), c(0, 4)))))
  register_geoarrow_udfs(functions = "ga_length_euclidean", prefix = "t12_")

  res <- dplyr::collect(dplyr::select(
    dplyr::mutate(lines, len = t12_ga_length_euclidean(geometry)),
    len
  ))

  expect_equal(res$len, 5)
})

test_that("a type the function cannot handle has no kernel", {
  tbl <- nc_table()
  register_geoarrow_udfs(
    crs = tbl,
    functions = "ga_length_euclidean",
    prefix = "t13_"
  )

  expect_error(
    dplyr::collect(dplyr::mutate(tbl, l = t13_ga_length_euclidean(geometry))),
    "no kernel matching"
  )
})

test_that("one call covers several coordinate reference systems", {
  tbl <- nc_table()
  plain <- vctr_table(sf::st_sfc(sf::st_point(c(0, 0)), sf::st_point(c(1, 1))))
  register_geoarrow_udfs(
    crs = list(NULL, tbl),
    functions = c("ga_unsigned_area", "ga_to_radians"),
    prefix = "t14_"
  )

  with_crs <- dplyr::collect(dplyr::select(
    dplyr::mutate(tbl, a = t14_ga_unsigned_area(geometry)),
    a
  ))
  without_crs <- dplyr::collect(dplyr::select(
    dplyr::mutate(plain, g = t14_ga_to_radians(geometry)),
    g
  ))

  expect_equal(nrow(with_crs), 100L)
  expect_equal(nrow(without_crs), 2L)
})

test_that("a CRS can be given as a string", {
  skip_arrow()
  registered <- register_geoarrow_udfs(
    crs = "OGC:CRS84",
    functions = "ga_to_radians",
    prefix = "t15_"
  )

  expect_equal(registered, "t15_ga_to_radians")
})

test_that("a name that is not in the catalogue is rejected", {
  tbl <- nc_table()

  expect_error(
    register_geoarrow_udfs(crs = tbl, functions = "ga_unary_union"),
    "Cannot register"
  )
})

test_that("an object with no geometry column is rejected", {
  skip_arrow()

  expect_error(
    register_geoarrow_udfs(crs = arrow::arrow_table(a = 1:2)),
    "no GeoArrow column"
  )
})

test_that("prefix is applied to every registered name", {
  tbl <- nc_table()
  registered <- register_geoarrow_udfs(
    crs = tbl,
    functions = c("ga_centroid", "ga_unsigned_area"),
    prefix = "t16_"
  )

  expect_setequal(registered, c("t16_ga_centroid", "t16_ga_unsigned_area"))
})

wkb_table <- function(sfc) {
  skip_arrow()
  skip_if_not_installed("wk")
  wkb <- arrow::Array$create(unclass(wk::as_wkb(sfc)), type = arrow::binary())
  arrow::arrow_table(id = seq_along(sfc), loc = wkb, other = wkb)
}

test_that("a bare WKB column dispatches without converting first", {
  pts <- sf::st_sfc(sf::st_point(c(0, 0)), sf::st_point(c(3, 4)))
  tbl <- wkb_table(pts)
  register_geoarrow_udfs(functions = "ga_unsigned_area", prefix = "w1_")

  res <- dplyr::collect(dplyr::select(
    dplyr::mutate(tbl, a = w1_ga_unsigned_area(loc)),
    a
  ))

  expect_equal(res$a, c(0, 0))
})

test_that("a type specific function reads bare WKB", {
  pts <- sf::st_sfc(sf::st_point(c(0, 0)), sf::st_point(c(3, 4)))
  tbl <- wkb_table(pts)
  register_geoarrow_udfs(
    functions = "ga_dist_euclidean_pairwise",
    prefix = "w2_"
  )

  res <- dplyr::collect(dplyr::select(
    dplyr::mutate(tbl, d = w2_ga_dist_euclidean_pairwise(loc, other)),
    d
  ))

  expect_equal(res$d, c(0, 0))
})

test_that("a geometry result from bare WKB comes back as WKB", {
  polys <- sf::st_sfc(
    sf::st_polygon(list(matrix(
      c(0, 0, 2, 0, 2, 2, 0, 2, 0, 0),
      ncol = 2,
      byrow = TRUE
    )))
  )
  tbl <- wkb_table(polys)
  register_geoarrow_udfs(functions = "ga_centroid", prefix = "w3_")

  res <- dplyr::collect(dplyr::select(
    dplyr::mutate(tbl, cen = w3_ga_centroid(loc)),
    cen
  ))

  expect_equal(
    as.character(wk::as_wkt(wk::as_wkb(res$cen))),
    "POINT (1 1)"
  )
})

test_that("WKB kernels do not disturb the GeoArrow ones", {
  tbl <- nc_table()
  registered <- register_geoarrow_udfs(
    crs = tbl,
    functions = "ga_unsigned_area",
    prefix = "w4_"
  )
  geo <- dplyr::collect(dplyr::select(
    dplyr::mutate(tbl, a = w4_ga_unsigned_area(geometry)),
    a
  ))

  expect_equal(registered, "w4_ga_unsigned_area")
  expect_equal(nrow(geo), 100L)
})
