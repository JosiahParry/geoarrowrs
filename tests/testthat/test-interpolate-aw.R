skip_if_not_installed("sf")
skip_if_not_installed("geoarrow")

# projected, since both sides measure planar area
nc_sf <- sf::st_set_agr(
  sf::st_transform(
    sf::read_sf(system.file("shape/nc.shp", package = "sf")),
    32119
  ),
  "constant"
)
counties <- geoarrow::as_geoarrow_array(sf::st_geometry(nc_sf))
cells_sf <- sf::st_make_grid(nc_sf, n = c(10, 5))
cells <- geoarrow::as_geoarrow_array(cells_sf)

# geo's overlay snaps each pair to an integer grid, so an overlap's area differs from GEOS's in the eighth digit
tol <- 1e-6

# sf's answer for every cell, NA where no county overlaps it
sf_aw <- function(x, to = cells_sf, ...) {
  sf::st_drop_geometry(sf::st_interpolate_aw(x, to, ..., keep_NA = TRUE))
}

sq <- function(x0, y0, x1, y1) {
  sprintf(
    "POLYGON ((%s %s, %s %s, %s %s, %s %s, %s %s))",
    x0,
    y0,
    x1,
    y0,
    x1,
    y1,
    x0,
    y1,
    x0,
    y0
  )
}
wkt <- function(...) geoarrow::as_geoarrow_array(wk::wkt(c(...)))

test_that("an extensive column matches sf row by row and keeps the total", {
  got <- ga_interpolate_aw(counties, nc_sf$BIR74, cells, extensive = TRUE)
  want <- sf_aw(nc_sf["BIR74"], extensive = TRUE)$BIR74

  expect_equal(as.vector(got), want, tolerance = tol)
  expect_identical(got$null_count, sum(is.na(want)))
  expect_equal(sum(as.vector(got), na.rm = TRUE), sum(nc_sf$BIR74))
})

test_that("an intensive column matches sf, over the covered or whole area", {
  got <- ga_interpolate_aw(counties, nc_sf$BIR74, cells, extensive = FALSE)
  want <- sf_aw(nc_sf["BIR74"], extensive = FALSE)$BIR74
  expect_equal(as.vector(got), want, tolerance = tol)

  got <- ga_interpolate_aw(
    counties,
    nc_sf$BIR74,
    cells,
    extensive = FALSE,
    include_non_intersected = TRUE
  )
  want <- sf_aw(
    nc_sf["BIR74"],
    extensive = FALSE,
    include_non_intersected = TRUE
  )$BIR74
  expect_equal(as.vector(got), want, tolerance = tol)
})

test_that("a table gives a struct array, one support per column", {
  cols <- c("BIR74", "SID74", "NWBIR74")
  ext <- c(TRUE, FALSE, TRUE)
  values <- sf::st_drop_geometry(nc_sf[cols])

  for (got in list(
    ga_interpolate_aw(counties, values, cells, extensive = ext),
    ga_interpolate_aw(counties, as.list(values), cells, extensive = ext),
    ga_interpolate_aw(
      counties,
      nanoarrow::as_nanoarrow_array(values),
      cells,
      extensive = ext
    )
  )) {
    res <- as.data.frame(got)
    expect_named(res, cols)
    for (k in seq_along(cols)) {
      want <- sf_aw(nc_sf[cols[k]], extensive = ext[k])[[cols[k]]]
      expect_equal(res[[cols[k]]], want, tolerance = tol)
    }
    # the struct is null in the cells no county overlaps
    expect_identical(got$null_count, sum(is.na(res$BIR74)))
  }
})

test_that("a null value reaches every target it overlaps, and na_rm drops it", {
  x <- nc_sf["BIR74"]
  x$BIR74[c(5, 40)] <- NA
  # assigning a column resets its agr
  x <- sf::st_set_agr(x, "constant")

  for (ext in c(TRUE, FALSE)) {
    got <- ga_interpolate_aw(counties, x$BIR74, cells, extensive = ext)
    expect_equal(
      as.vector(got),
      sf_aw(x, extensive = ext)$BIR74,
      tolerance = tol
    )

    got <- ga_interpolate_aw(
      counties,
      x$BIR74,
      cells,
      extensive = ext,
      na_rm = TRUE
    )
    expect_equal(
      as.vector(got),
      sf_aw(x, extensive = ext, na.rm = TRUE)$BIR74,
      tolerance = tol
    )
  }
})

test_that("weights match sf's dasymetric interpolation and keep the total", {
  withr::local_seed(1)
  to_sf <- sf::st_set_agr(
    sf::st_sf(w = stats::runif(length(cells_sf)), geometry = cells_sf),
    "constant"
  )
  got <- ga_interpolate_aw(
    counties,
    nc_sf$BIR74,
    cells,
    extensive = TRUE,
    weights = to_sf$w
  )
  want <- sf::st_interpolate_aw(
    nc_sf["BIR74"],
    to_sf,
    extensive = TRUE,
    weights = "w"
  )
  rows <- as.integer(rownames(want))

  expect_equal(as.vector(got)[rows], want$BIR74, tolerance = tol)
  expect_true(all(is.na(as.vector(got)[-rows])))
  expect_equal(sum(as.vector(got), na.rm = TRUE), sum(nc_sf$BIR74))
})

test_that("weights match sf when the targets overlap each other", {
  withr::local_seed(2)
  bb <- sf::st_bbox(nc_sf)
  shifted <- cells_sf +
    c((bb[["xmax"]] - bb[["xmin"]]) / 20, (bb[["ymax"]] - bb[["ymin"]]) / 10)
  sf::st_crs(shifted) <- sf::st_crs(nc_sf)
  to_sf <- sf::st_set_agr(
    sf::st_sf(
      w = stats::runif(2 * length(cells_sf)),
      geometry = c(cells_sf, shifted)
    ),
    "constant"
  )

  got <- ga_interpolate_aw(
    counties,
    nc_sf$BIR74,
    geoarrow::as_geoarrow_array(sf::st_geometry(to_sf)),
    extensive = TRUE,
    weights = to_sf$w
  )
  want <- sf::st_interpolate_aw(
    nc_sf["BIR74"],
    to_sf,
    extensive = TRUE,
    weights = "w"
  )
  expect_equal(
    as.vector(got)[as.integer(rownames(want))],
    want$BIR74,
    tolerance = tol
  )
})

test_that("allocate_total hands out a source's whole value to the targets it meets", {
  src <- wkt(sq(0, 0, 2, 2), sq(2, 0, 4, 2))
  # the first target covers half of the first source, and nothing covers the rest
  tgt <- wkt(sq(0, 0, 1, 2), sq(2, 0, 4, 2))

  expect_equal(
    as.vector(ga_interpolate_aw(src, c(10, 20), tgt, extensive = TRUE)),
    c(5, 20)
  )
  expect_equal(
    as.vector(ga_interpolate_aw(
      src,
      c(10, 20),
      tgt,
      extensive = TRUE,
      allocate_total = TRUE
    )),
    c(10, 20)
  )
})

test_that("a target with no overlap of any area is null", {
  src <- wkt(sq(0, 0, 2, 2), sq(2, 0, 4, 2))
  tgt <- wkt(
    sq(1, 0, 3, 2),
    # touches the second source along x = 4
    sq(4, 0, 5, 2),
    NA,
    sq(10, 10, 11, 11),
    sq(0, 0, 1, 1)
  )

  got <- ga_interpolate_aw(src, c(10, 20), tgt, extensive = TRUE)
  expect_equal(as.vector(got), c(15, NA, NA, NA, 2.5))
  expect_identical(got$null_count, 3L)

  got <- ga_interpolate_aw(src, c(10, 20), tgt, extensive = FALSE)
  expect_equal(as.vector(got), c(15, NA, NA, NA, 10))

  # a single value recycles over every source
  got <- ga_interpolate_aw(src, 1, tgt, extensive = FALSE)
  expect_equal(as.vector(got), c(1, NA, NA, NA, 1))
})

test_that("points cover nothing and boxes read as polygons", {
  src <- wkt(sq(0, 0, 2, 2), sq(2, 0, 4, 2))
  tgt <- wkt(sq(1, 0, 3, 2))
  pts <- ga_point(c(1, 3), c(1, 1))

  expect_identical(
    ga_interpolate_aw(pts, c(10, 20), tgt, extensive = TRUE)$null_count,
    1L
  )
  expect_identical(
    ga_interpolate_aw(src, c(10, 20), pts, extensive = TRUE)$null_count,
    2L
  )
  expect_equal(
    as.vector(ga_interpolate_aw(
      ga_envelope(src),
      c(10, 20),
      tgt,
      extensive = TRUE
    )),
    15
  )
})

test_that("ga_interpolate_aw rejects arguments it cannot use", {
  src <- wkt(sq(0, 0, 2, 2), sq(2, 0, 4, 2))
  tgt <- wkt(sq(1, 0, 3, 2))

  expect_error(
    ga_interpolate_aw(src, 1:3, tgt, extensive = TRUE),
    "`values` and `x` must be the same length"
  )
  expect_error(
    ga_interpolate_aw(src, 1:2, tgt, extensive = NA),
    "TRUE or FALSE"
  )
  expect_error(ga_interpolate_aw(src, 1:2, tgt, extensive = 1), "TRUE or FALSE")
  expect_error(
    ga_interpolate_aw(
      src,
      data.frame(a = 1:2, b = 1:2),
      tgt,
      extensive = c(TRUE, FALSE, TRUE)
    ),
    "one per column"
  )
  expect_error(
    ga_interpolate_aw(src, data.frame(a = c("x", "y")), tgt, extensive = TRUE),
    "column `a` of `values` must be numeric"
  )
  expect_error(
    ga_interpolate_aw(
      src,
      data.frame(a = factor(c("x", "y"))),
      tgt,
      extensive = TRUE
    ),
    "must be numeric"
  )
  expect_error(
    ga_interpolate_aw(src, data.frame(), tgt, extensive = TRUE),
    "at least one column"
  )
  expect_error(
    ga_interpolate_aw(src, 1:2, tgt, extensive = FALSE, weights = 1),
    "extensive columns only"
  )
  expect_error(
    ga_interpolate_aw(src, 1:2, tgt, extensive = TRUE, weights = 1:3),
    "`weights` and `to` must be the same length"
  )
})

test_that("sf's st_interpolate_aw dispatches on a data frame to ga_interpolate_aw", {
  cols <- c("BIR74", "SID74")
  nc <- data.frame(
    sf::st_drop_geometry(nc_sf[cols]),
    geometry = geoarrow::as_geoarrow_vctr(counties)
  )
  vctr <- geoarrow::as_geoarrow_vctr(cells)

  for (keep in c(FALSE, TRUE)) {
    for (ext in c(TRUE, FALSE)) {
      got <- sf::st_interpolate_aw(nc, vctr, extensive = ext, keep_NA = keep)
      want <- sf::st_interpolate_aw(
        nc_sf[cols],
        cells_sf,
        extensive = ext,
        keep_NA = keep
      )
      expect_s3_class(got$geometry, "geoarrow_vctr")
      expect_identical(rownames(got), rownames(want))
      expect_equal(got$BIR74, want$BIR74, tolerance = tol)
      expect_equal(got$SID74, want$SID74, tolerance = tol)
    }
  }

  to <- data.frame(w = seq_along(vctr), geometry = vctr)
  to_sf <- sf::st_set_agr(
    sf::st_sf(w = to$w, geometry = cells_sf),
    "constant"
  )
  got <- sf::st_interpolate_aw(
    nc[c("BIR74", "geometry")],
    to,
    extensive = TRUE,
    weights = "w"
  )
  want <- sf::st_interpolate_aw(
    nc_sf["BIR74"],
    to_sf,
    extensive = TRUE,
    weights = "w"
  )
  expect_identical(rownames(got), rownames(want))
  expect_equal(got$BIR74, want$BIR74, tolerance = tol)

  expect_error(
    sf::st_interpolate_aw(
      data.frame(n = c("a", "b"), geometry = vctr[1:2]),
      vctr,
      TRUE
    ),
    "non-numeric"
  )
  expect_error(
    sf::st_interpolate_aw(nc, to, extensive = TRUE, weights = "nope"),
    "must name one column"
  )
})
