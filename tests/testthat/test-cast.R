skip_deps <- function() {
  skip_if_not_installed("sf")
  skip_if_not_installed("nanoarrow")
  skip_if_not_installed("geoarrow")
}

ga <- function(sfc) {
  skip_deps()
  requireNamespace("geoarrow", quietly = TRUE)
  geoarrow::as_geoarrow_array(sfc)
}

ext <- function(a) {
  nanoarrow::infer_nanoarrow_schema(a)$metadata[["ARROW:extension:name"]]
}

child_ext <- function(a) {
  nanoarrow::infer_nanoarrow_schema(a)$children[[1]]$metadata[[
    "ARROW:extension:name"
  ]]
}

pts <- function() ga(sf::st_sfc(sf::st_point(c(0, 0)), sf::st_point(c(1, 1))))

square <- function(o = 0) {
  sf::st_polygon(list(matrix(
    c(o, o, o + 1, o, o + 1, o + 1, o, o + 1, o, o),
    ncol = 2,
    byrow = TRUE
  )))
}

test_that("singular types cast up to their multi form", {
  expect_equal(
    ext(ga_cast_geometry(pts(), "multipoint")),
    "geoarrow.multipoint"
  )

  ls <- ga(sf::st_sfc(sf::st_linestring(matrix(
    c(0, 0, 1, 1),
    ncol = 2,
    byrow = TRUE
  ))))
  expect_equal(
    ext(ga_cast_geometry(ls, "multilinestring")),
    "geoarrow.multilinestring"
  )

  poly <- ga(sf::st_sfc(square()))
  expect_equal(
    ext(ga_cast_geometry(poly, "multipolygon")),
    "geoarrow.multipolygon"
  )
})

test_that("any type casts up to geometry, wkb and wkt", {
  expect_equal(ext(ga_cast_geometry(pts(), "geometry")), "geoarrow.geometry")
  expect_equal(ext(ga_cast_geometry(pts(), "wkb")), "geoarrow.wkb")
  expect_equal(ext(ga_cast_geometry(pts(), "wkt")), "geoarrow.wkt")
})

test_that("casting preserves length and geometries", {
  out <- ga_cast_geometry(pts(), "multipoint")
  sfc <- sf::st_as_sfc(geoarrow::as_geoarrow_vctr(out))

  expect_length(sfc, 2L)
  expect_equal(
    as.numeric(sf::st_coordinates(sfc)[, c("X", "Y")]),
    c(0, 1, 0, 1)
  )
})

test_that("an unknown target type errors", {
  expect_error(ga_cast_geometry(pts(), "hexagon"), "`to` must be one of")
})

test_that("downcast narrows a geometry array to its concrete type", {
  wide <- ga_cast_geometry(pts(), "geometry")
  expect_equal(ext(wide), "geoarrow.geometry")
  expect_equal(ext(ga_downcast_geometry(wide)), "geoarrow.point")
})

test_that("downcast leaves a genuinely mixed array alone", {
  # geoarrow-r encodes a mixed sfc as wkb, so go via geometry explicitly
  mixed <- ga_cast_geometry(
    ga(sf::st_sfc(sf::st_point(c(0, 0)), square())),
    "geometry"
  )
  expect_equal(ext(mixed), "geoarrow.geometry")
  expect_equal(ext(ga_downcast_geometry(mixed)), "geoarrow.geometry")
})

test_that("explode preserves length and yields the singular type", {
  mp <- ga(sf::st_sfc(
    sf::st_multipoint(matrix(c(0, 0, 1, 1, 2, 2), ncol = 2, byrow = TRUE)),
    sf::st_multipoint(matrix(c(5, 5), ncol = 2))
  ))
  res <- ga_explode(mp)

  expect_equal(child_ext(res), "geoarrow.point")
  expect_equal(length(nanoarrow::convert_array(res)), 2L)
  expect_equal(lengths(nanoarrow::convert_array(res)), c(3L, 1L))
})

test_that("explode uses the declared type, not the contents", {
  # every element holds a single part, but the array is still multipolygon
  mp <- ga(sf::st_sfc(sf::st_multipolygon(list(square()))))
  expect_equal(ext(mp), "geoarrow.multipolygon")
  expect_equal(child_ext(ga_explode(mp)), "geoarrow.polygon")
})

test_that("explode of a singular array gives one part per row", {
  poly <- ga(sf::st_sfc(square(0), square(5)))
  res <- ga_explode(poly)

  expect_equal(child_ext(res), "geoarrow.polygon")
  expect_equal(lengths(nanoarrow::convert_array(res)), c(1L, 1L))
})

test_that("explode errors on a mixed geometry array", {
  wide <- ga_cast_geometry(pts(), "geometry")
  expect_error(ga_explode(wide), "narrow it to a single geometry type")
})

test_that("flatten collapses an exploded list", {
  mp <- ga(sf::st_sfc(
    sf::st_multipoint(matrix(c(0, 0, 1, 1, 2, 2), ncol = 2, byrow = TRUE)),
    sf::st_multipoint(matrix(c(5, 5), ncol = 2))
  ))
  flat <- ga_flatten(ga_explode(mp))

  expect_equal(ext(flat), "geoarrow.point")
  expect_length(sf::st_as_sfc(geoarrow::as_geoarrow_vctr(flat)), 4L)
})

test_that("explode then flatten round trips a multipolygon column", {
  g <- as.data.frame(
    read_shapefile(system.file("shape/nc.shp", package = "sf"))
  )$geometry

  parts <- ga_explode(g)
  expect_equal(length(nanoarrow::convert_array(parts)), 100L)

  flat <- ga_flatten(parts)
  n_parts <- sum(lengths(nanoarrow::convert_array(parts)))
  expect_length(sf::st_as_sfc(geoarrow::as_geoarrow_vctr(flat)), n_parts)
})

test_that("flatten rejects something that is not a list array", {
  expect_error(ga_flatten(pts()), "list array")
})
