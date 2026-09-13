skip_if_not_installed("sf")
skip_if_not_installed("geoarrow")

nc <- as.data.frame(read_shapefile(
  system.file("shape/nc.shp", package = "sf")
))
geom <- geoarrow::as_geoarrow_vctr(nc$geometry)
cent <- sf::st_centroid(geom)

test_that("sf's generics dispatch on geoarrow_vctr", {
  expect_identical(
    as.vector(sf::st_area(geom)),
    as.vector(ga_unsigned_area(geom))
  )
  expect_identical(
    as.vector(sf::st_is_valid(geom)),
    as.vector(ga_is_valid(geom))
  )
})

test_that("every geometry result is a geoarrow_vctr", {
  expect_s3_class(sf::st_centroid(geom), "geoarrow_vctr")
  expect_s3_class(sf::st_convex_hull(geom), "geoarrow_vctr")
  expect_s3_class(sf::st_point_on_surface(geom), "geoarrow_vctr")
  expect_s3_class(sf::st_buffer(geom[1:3], 0.1), "geoarrow_vctr")
  expect_s3_class(
    sf::st_simplify(geom[1:3], dTolerance = 0.05),
    "geoarrow_vctr"
  )
  expect_s3_class(sf::st_union(geom), "geoarrow_vctr")
  expect_s3_class(sf::st_triangulate(geom[1:3]), "geoarrow_vctr")
  expect_s3_class(sf::st_voronoi(cent[1:10]), "geoarrow_vctr")
})

test_that("a geometrycollection result is still a geoarrow_vctr", {
  # geoarrow does not know the extension name, so this one goes through WKB
  mask_sf()
  on.exit(detach("geoarrowrs:sf"), add = TRUE)

  combined <- st_combine(geom)
  expect_s3_class(combined, "geoarrow_vctr")
  expect_identical(length(combined), 1L)

  # and the result feeds the next call rather than dead ending
  expect_equal(
    as.vector(ga_unsigned_area(sf::st_convex_hull(combined))),
    as.vector(ga_unsigned_area(ga_convex_hull(ga_collect_agg(geom))))
  )
})

test_that("st_union is unary with one argument and binary with two", {
  expect_identical(length(sf::st_union(geom)), 1L)
  expect_identical(length(sf::st_union(geom[1:3], geom[4:6])), 3L)
})

test_that("mask_sf() attaches the non-generics and detaches cleanly", {
  mask_sf()
  expect_true("geoarrowrs:sf" %in% search())
  expect_true(exists("st_within", where = "geoarrowrs:sf", inherits = FALSE))
  # a generic is not attached, since masking it would shadow sf's dispatch
  expect_false(exists("st_area", where = "geoarrowrs:sf", inherits = FALSE))

  detach("geoarrowrs:sf")
  expect_false("geoarrowrs:sf" %in% search())
})

test_that("mask_sf() twice does not stack on the search path", {
  mask_sf()
  mask_sf()
  on.exit(detach("geoarrowrs:sf"), add = TRUE)

  expect_identical(sum(search() == "geoarrowrs:sf"), 1L)
})

test_that("the masked predicates match the sparse predicates", {
  mask_sf()
  on.exit(detach("geoarrowrs:sf"), add = TRUE)

  expect_identical(
    as.vector(st_within(geom[1:5], geom[1:12])),
    as.vector(ga_sparse_within(geom[1:5], geom[1:12]))
  )
  expect_identical(
    as.vector(st_touches(geom[1:5], geom[1:12])),
    as.vector(ga_sparse_touches(geom[1:5], geom[1:12]))
  )
})

test_that("sparse = FALSE gives the dense matrix sf's argument asks for", {
  dense <- sf::st_intersects(geom[1:4], geom[1:12], sparse = FALSE)
  expect_true(is.matrix(dense))
  expect_identical(dim(dense), c(4L, 12L))

  sparse <- as.vector(sf::st_intersects(geom[1:4], geom[1:12]))
  expect_identical(which(dense[1, ]), as.integer(sparse[[1]]))
})

test_that("st_disjoint is the complement of st_intersects", {
  mask_sf()
  on.exit(detach("geoarrowrs:sf"), add = TRUE)

  hits <- as.vector(sf::st_intersects(geom[1:4], geom[1:12]))
  miss <- st_disjoint(geom[1:4], geom[1:12])

  for (i in seq_len(4)) {
    expect_identical(
      sort(c(as.integer(hits[[i]]), miss[[i]])),
      seq_len(12L)
    )
  }
})

test_that("st_distance measures by element and across", {
  mask_sf()
  on.exit(detach("geoarrowrs:sf"), add = TRUE)

  expect_identical(
    as.vector(st_distance(cent[1:3], cent[4:6], by_element = TRUE)),
    as.vector(ga_dist_euclidean_pairwise(cent[1:3], cent[4:6]))
  )
  expect_length(as.vector(st_distance(cent[1:3], cent[1:5]))[[1]], 5L)
})

test_that("an sfc is refused rather than converted", {
  mask_sf()
  on.exit(detach("geoarrowrs:sf"), add = TRUE)

  sfc <- sf::st_geometry(sf::st_read(
    system.file("shape/nc.shp", package = "sf"),
    quiet = TRUE
  ))
  expect_error(st_length(sfc), "geoarrow_vctr")
})
