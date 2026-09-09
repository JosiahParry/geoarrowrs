skip_if_no_sf <- function() {
  skip_if_not_installed("sf")
  skip_if_not_installed("nanoarrow")
}

write_geojson <- function(text) {
  path <- tempfile(fileext = ".geojson")
  writeLines(text, path)
  path
}

feature_collection <- function(features) {
  paste0('{"type":"FeatureCollection","features":[', paste(features, collapse = ","), "]}")
}

feat <- function(geom, props = "{}") {
  paste0('{"type":"Feature","geometry":', geom, ',"properties":', props, "}")
}

pt <- function(x, y) paste0('{"type":"Point","coordinates":[', x, ",", y, "]}")

test_that("properties and geometry round trip", {
  skip_if_no_sf()

  path <- write_geojson(feature_collection(c(
    feat(pt(0, 1), '{"name":"a","n":1,"ok":true}'),
    feat(pt(2, 3), '{"name":"b","n":2,"ok":false}')
  )))

  df <- as.data.frame(nanoarrow::convert_array_stream(read_geojson(path)))

  expect_equal(nrow(df), 2L)
  expect_equal(names(df), c("name", "n", "ok", "geometry"))
  expect_equal(df$name, c("a", "b"))
  expect_equal(df$n, c(1L, 2L))
  expect_equal(df$ok, c(TRUE, FALSE))
})

test_that("property columns keep first-seen key order", {
  skip_if_no_sf()

  path <- write_geojson(feature_collection(c(
    feat(pt(0, 0), '{"zebra":1,"apple":2}')
  )))

  df <- as.data.frame(nanoarrow::convert_array_stream(read_geojson(path)))
  expect_equal(names(df), c("zebra", "apple", "geometry"))
})

# arrow format strings: "l" int64, "g" float64, "u" utf8
col_format <- function(path, name) {
  nanoarrow::infer_nanoarrow_schema(read_geojson(path))$children[[name]]$format
}

test_that("integers stay int64 but widen to double alongside reals", {
  skip_if_no_sf()

  ints <- write_geojson(feature_collection(c(
    feat(pt(0, 0), '{"v":1}'), feat(pt(1, 1), '{"v":2}')
  )))
  reals <- write_geojson(feature_collection(c(
    feat(pt(0, 0), '{"v":1}'), feat(pt(1, 1), '{"v":2.5}')
  )))

  # R has no int64, so check the arrow type rather than the converted R type
  expect_equal(col_format(ints, "v"), "l")
  expect_equal(col_format(reals, "v"), "g")

  v <- as.data.frame(nanoarrow::convert_array_stream(read_geojson(reals)))$v
  expect_equal(v, c(1, 2.5))
})

test_that("types that cannot unify fall back to string", {
  skip_if_no_sf()

  path <- write_geojson(feature_collection(c(
    feat(pt(0, 0), '{"v":1}'), feat(pt(1, 1), '{"v":"text"}')
  )))

  v <- as.data.frame(nanoarrow::convert_array_stream(read_geojson(path)))$v
  expect_type(v, "character")
  expect_equal(v, c("1", "text"))
})

test_that("nested arrays and objects are kept as JSON text", {
  skip_if_no_sf()

  path <- write_geojson(feature_collection(c(
    feat(pt(0, 0), '{"tags":["a","b"],"meta":{"k":1}}')
  )))

  df <- as.data.frame(nanoarrow::convert_array_stream(read_geojson(path)))
  expect_equal(df$tags, '["a","b"]')
  expect_equal(df$meta, '{"k":1}')
})

test_that("a key missing from a feature is null for that row", {
  skip_if_no_sf()

  path <- write_geojson(feature_collection(c(
    feat(pt(0, 0), '{"a":"x"}'),
    feat(pt(1, 1), '{"b":"y"}')
  )))

  df <- as.data.frame(nanoarrow::convert_array_stream(read_geojson(path)))
  expect_equal(df$a, c("x", NA))
  expect_equal(df$b, c(NA, "y"))
})

test_that("a null geometry becomes a null element", {
  skip_if_no_sf()

  path <- write_geojson(feature_collection(c(
    feat(pt(0, 0), '{"id":1}'),
    feat("null", '{"id":2}')
  )))

  skip_if_not_installed("geoarrow")
  requireNamespace("geoarrow", quietly = TRUE)
  df <- as.data.frame(read_geojson(path))
  sfc <- sf::st_as_sfc(df$geometry)

  expect_equal(nrow(df), 2L)
  expect_equal(df$id, c(1, 2))
  expect_equal(sf::st_is_empty(sfc), c(FALSE, TRUE))
})

geom_ext <- function(path) {
  nanoarrow::infer_nanoarrow_schema(
    read_geojson(path)
  )$children$geometry$metadata[["ARROW:extension:name"]]
}

test_that("a homogeneous collection narrows to a concrete geometry type", {
  skip_if_no_sf()

  pts <- write_geojson(feature_collection(c(feat(pt(0, 0)), feat(pt(1, 1)))))
  expect_equal(geom_ext(pts), "geoarrow.point")

  polys <- write_geojson(feature_collection(c(
    feat('{"type":"Polygon","coordinates":[[[0,0],[1,0],[1,1],[0,0]]]}')
  )))
  expect_equal(geom_ext(polys), "geoarrow.polygon")
})

test_that("a polygon/multipolygon mix promotes to multipolygon", {
  skip_if_no_sf()

  path <- write_geojson(feature_collection(c(
    feat('{"type":"Polygon","coordinates":[[[0,0],[1,0],[1,1],[0,0]]]}'),
    feat('{"type":"MultiPolygon","coordinates":[[[[5,5],[6,5],[6,6],[5,5]]]]}')
  )))

  expect_equal(geom_ext(path), "geoarrow.multipolygon")
})

test_that("genuinely mixed families fall back to geoarrow.geometry", {
  skip_if_no_sf()

  path <- write_geojson(feature_collection(c(
    feat(pt(0, 0), '{"id":1}'),
    feat('{"type":"LineString","coordinates":[[0,0],[1,1]]}', '{"id":2}'),
    feat('{"type":"Polygon","coordinates":[[[0,0],[1,0],[1,1],[0,0]]]}', '{"id":3}')
  )))

  # geoarrow's R bindings cannot read this union type yet, so assert at the
  # arrow level rather than converting to sf
  expect_equal(geom_ext(path), "geoarrow.geometry")

  df <- as.data.frame(nanoarrow::convert_array_stream(read_geojson(path)))
  expect_equal(nrow(df), 3L)
})

test_that("the geometry column is tagged OGC:CRS84", {
  skip_if_no_sf()

  path <- write_geojson(feature_collection(c(feat(pt(0, 0)))))
  schema <- nanoarrow::infer_nanoarrow_schema(read_geojson(path))
  meta <- schema$children$geometry$metadata[["ARROW:extension:metadata"]]

  expect_match(meta, "CRS84", fixed = TRUE)
})

test_that("reading a missing file errors rather than crashing", {
  expect_error(read_geojson(tempfile(fileext = ".geojson")))
})
