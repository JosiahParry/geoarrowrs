skip_if_not_installed("sf")
skip_if_not_installed("geoarrow")

# one point carrying crs, the way geoarrow writes it from sf
point_in <- function(crs) {
  geoarrow::as_geoarrow_array(sf::st_sfc(sf::st_point(c(1, 1)), crs = crs))
}

test_that("ga_is_longlat() agrees with sf for each kind of crs", {
  crs <- c(
    "EPSG:4326",
    # geographic 3D
    "EPSG:4979",
    "OGC:CRS84",
    # NAD27, on the Clarke 1866 ellipsoid
    "EPSG:4267",
    "EPSG:32119",
    "EPSG:3857",
    "ESRI:54009",
    # geocentric, in metres from the centre of the Earth
    "EPSG:4978",
    # compound, projected then vertical
    "EPSG:5972",
    # compound, geographic then vertical
    "EPSG:9518",
    # the Moon, geographic and then projected
    "IAU_2015:30100",
    "IAU_2015:30110",
    # bound, a geographic CRS with a datum shift to WGS 84
    "+proj=longlat +ellps=intl +towgs84=-87,-98,-121 +no_defs",
    # derived geographic, a rotated pole
    "+proj=ob_tran +o_proj=longlat +o_lon_p=0 +o_lat_p=30 +lon_0=0 +datum=WGS84 +no_defs"
  )
  for (x in crs) {
    expect_identical(
      ga_is_longlat(point_in(x)),
      sf::st_is_longlat(sf::st_crs(x)),
      label = x
    )
  }
})

test_that("ga_is_longlat() reads a crs a crs argument stores as PROJJSON", {
  expect_true(ga_is_longlat(ga_point(1, 1, crs = sf::st_crs(4326)$ProjJson)))
  expect_false(ga_is_longlat(ga_point(1, 1, crs = sf::st_crs(32119)$ProjJson)))
})

test_that("ga_is_longlat() returns NA without a PROJJSON crs", {
  no_crs <- geoarrow::as_geoarrow_array(sf::st_sfc(sf::st_point(c(1, 1))))
  expect_identical(ga_is_longlat(no_crs), NA)
  expect_identical(ga_is_longlat(ga_point(1, 1)), NA)
  expect_identical(ga_is_longlat(ga_point(1, 1, crs = "EPSG:4326")), NA)
  expect_identical(
    ga_is_longlat(ga_point(1, 1, crs = sf::st_crs(4326)$wkt)),
    NA
  )
})

test_that("ga_is_longlat() reads the crs of every geometry type", {
  nc <- sf::read_sf(system.file("shape/nc.shp", package = "sf"))
  geoms <- list(
    sf::st_geometry(nc),
    sf::st_cast(sf::st_geometry(nc), "POLYGON"),
    sf::st_boundary(sf::st_geometry(nc)),
    suppressWarnings(sf::st_centroid(sf::st_geometry(nc)))
  )
  for (g in geoms) {
    expect_true(ga_is_longlat(geoarrow::as_geoarrow_array(g)))
    projected <- geoarrow::as_geoarrow_array(sf::st_transform(g, 32119))
    expect_false(ga_is_longlat(projected))
  }
  wkb <- ga_cast_geometry(geoarrow::as_geoarrow_array(geoms[[1]]), "wkb")
  expect_true(ga_is_longlat(wkb))
})

test_that("the masked st_is_longlat() calls ga_is_longlat()", {
  g <- geoarrow::as_geoarrow_vctr(point_in("EPSG:4326"))
  expect_identical(st_is_longlat(g), ga_is_longlat(g))
})
