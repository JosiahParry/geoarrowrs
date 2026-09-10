test_that("no cap applies in an ordinary session", {
  withr::with_envvar(
    c("_R_CHECK_LIMIT_CORES_" = NA, "OMP_THREAD_LIMIT" = NA),
    expect_null(geoarrowrs_thread_cap())
  )
})

test_that("R CMD check caps at two cores", {
  withr::with_envvar(
    c("_R_CHECK_LIMIT_CORES_" = "TRUE", "OMP_THREAD_LIMIT" = NA),
    expect_equal(geoarrowrs_thread_cap(), 2L)
  )
})

test_that("OMP_THREAD_LIMIT is respected as a ceiling", {
  withr::with_envvar(
    c("_R_CHECK_LIMIT_CORES_" = NA, "OMP_THREAD_LIMIT" = "4"),
    expect_equal(geoarrowrs_thread_cap(), 4L)
  )
})

test_that("the check flag wins over OMP_THREAD_LIMIT", {
  withr::with_envvar(
    c("_R_CHECK_LIMIT_CORES_" = "TRUE", "OMP_THREAD_LIMIT" = "8"),
    expect_equal(geoarrowrs_thread_cap(), 2L)
  )
})

test_that("a nonsense OMP_THREAD_LIMIT is ignored", {
  withr::with_envvar(
    c("_R_CHECK_LIMIT_CORES_" = NA, "OMP_THREAD_LIMIT" = "nope"),
    expect_null(suppressWarnings(geoarrowrs_thread_cap()))
  )
})

test_that("the thread pool option does not change the answer", {
  skip_if_not_installed("sf")
  skip_if_not_installed("geoarrow")
  skip_if_not_installed("nanoarrow")
  sfc <- sf::st_sfc(sf::st_point(c(0, 0)), sf::st_point(c(3, 4)))
  g <- geoarrow::as_geoarrow_array(sfc)

  serial <- withr::with_options(
    list(geoarrowrs.thread_pool = 1L),
    as.character(geoarrow::as_geoarrow_vctr(ga_cast_geometry(g, "multipoint")))
  )
  parallel <- withr::with_options(
    list(geoarrowrs.thread_pool = NULL),
    as.character(geoarrow::as_geoarrow_vctr(ga_cast_geometry(g, "multipoint")))
  )

  expect_equal(serial, parallel)
})

test_that("a zero or negative thread pool falls back to all cores", {
  skip_if_not_installed("sf")
  skip_if_not_installed("geoarrow")
  skip_if_not_installed("nanoarrow")
  g <- geoarrow::as_geoarrow_array(sf::st_sfc(sf::st_point(c(0, 0))))

  expect_no_error(withr::with_options(
    list(geoarrowrs.thread_pool = 0L),
    ga_cast_geometry(g, "multipoint")
  ))
  expect_no_error(withr::with_options(
    list(geoarrowrs.thread_pool = -1L),
    ga_cast_geometry(g, "multipoint")
  ))
})
