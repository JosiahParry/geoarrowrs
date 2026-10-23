skip_deps <- function() {
  skip_if_not_installed("sf")
  skip_if_not_installed("geoarrow")
}

ga <- function(sfc) {
  skip_deps()
  geoarrow::as_geoarrow_array(sfc)
}

as_sfc <- function(x) sf::st_as_sfc(geoarrow::as_geoarrow_vctr(x))

sq <- rbind(c(0, 0), c(1, 0), c(1, 1), c(0, 1), c(0, 0))

rows <- function() {
  sf::st_sfc(
    sf::st_geometrycollection(list(
      sf::st_point(c(5, 5)),
      sf::st_polygon(list(sq)),
      sf::st_linestring(rbind(c(0, 0), c(1, 1))),
      sf::st_polygon(list(sq + 2)),
      sf::st_multipoint(rbind(c(7, 7), c(8, 8))),
      sf::st_multilinestring(list(rbind(c(3, 3), c(4, 4))))
    )),
    sf::st_polygon(list(sq)),
    sf::st_multipolygon(list(list(sq), list(sq + 3))),
    sf::st_linestring(rbind(c(0, 0), c(1, 1))),
    sf::st_point(c(9, 9)),
    sf::st_geometrycollection()
  )
}

# sf drops or errors on a row with no parts of the type, so compare row by row
sf_parts <- function(x, type) {
  out <- tryCatch(
    suppressWarnings(sf::st_collection_extract(x, type)),
    error = function(e) NULL
  )
  if (length(out) == 0) {
    return(character())
  }
  sf::st_as_text(sf::st_cast(out, type))
}

test_that("collection_extract keeps the parts sf keeps, one row per input", {
  x <- rows()
  for (type in c("POLYGON", "LINESTRING", "POINT")) {
    arr <- ga_collection_extract(ga(x), type)
    types <- as.vector(ga_geometry_type(arr))
    res <- as_sfc(arr)

    expect_length(types, length(x))
    expect_setequal(types[!is.na(types)], paste0("MULTI", type))
    for (i in seq_along(x)) {
      expected <- sf_parts(x[i], type)
      if (length(expected) == 0) {
        expect_true(is.na(types[i]))
      } else {
        expect_equal(sf::st_as_text(sf::st_cast(res[i], type)), expected)
      }
    }
  }
})

test_that("collection_extract defaults to polygons", {
  x <- rows()
  expect_equal(
    as_sfc(ga_collection_extract(ga(x))),
    as_sfc(ga_collection_extract(ga(x), "POLYGON"))
  )
})

test_that("collection_extract flattens a nested collection", {
  skip_deps()
  g <- geoarrow::as_geoarrow_array(wk::wkt(paste(
    "GEOMETRYCOLLECTION (GEOMETRYCOLLECTION (POLYGON ((0 0, 1 0, 1 1, 0 0))),",
    "POINT (9 9), POLYGON ((5 5, 6 5, 6 6, 5 5)))"
  )))

  expect_equal(
    sf::st_as_text(as_sfc(ga_collection_extract(g, "POLYGON"))),
    "MULTIPOLYGON (((0 0, 1 0, 1 1, 0 0)), ((5 5, 6 5, 6 6, 5 5)))"
  )
})

test_that("collection_extract is null where there is nothing to extract", {
  skip_deps()
  g <- geoarrow::as_geoarrow_array(wk::wkt(c(
    "POINT (0 0)",
    NA,
    "GEOMETRYCOLLECTION EMPTY",
    "LINESTRING (0 0, 1 1)"
  )))
  res <- ga_collection_extract(g, "POINT")

  expect_equal(res$length, 4L)
  expect_equal(
    as.vector(ga_geometry_type(res)),
    c("MULTIPOINT", NA, NA, NA)
  )
})

test_that("collection_extract rejects an unknown type", {
  g <- ga(sf::st_sfc(sf::st_point(c(0, 0))))
  expect_error(ga_collection_extract(g, "MULTIPOLYGON"), "geometry_type")
  expect_error(ga_collection_extract(g, "POLYGN"), "POLYGN")
})
