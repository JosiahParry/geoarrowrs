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

conv <- function(x) as.vector(x)

box <- function(xmin, ymin, xmax, ymax) {
  sf::st_polygon(list(matrix(
    c(xmin, ymin, xmax, ymin, xmax, ymax, xmin, ymax, xmin, ymin),
    ncol = 2,
    byrow = TRUE
  )))
}
pt <- function(x, y) sf::st_point(c(x, y))

big <- function() box(0, 0, 4, 4)
small <- function() box(1, 1, 2, 2)

test_that("contains and within are mirror images", {
  x <- ga(sf::st_sfc(big()))
  y <- ga(sf::st_sfc(small()))

  expect_true(conv(ga_contains(x, y)))
  expect_false(conv(ga_within(x, y)))
  expect_true(conv(ga_within(y, x)))
})

test_that("contains matches sf", {
  outer <- sf::st_sfc(big())
  inner <- sf::st_sfc(small())

  expect_equal(
    conv(ga_contains(ga(outer), ga(inner))),
    sf::st_contains(outer, inner, sparse = FALSE)[1, 1]
  )
})

test_that("intersects and disjoint are negations", {
  x <- ga(sf::st_sfc(big(), big()))
  y <- ga(sf::st_sfc(small(), box(9, 9, 10, 10)))

  expect_equal(conv(ga_intersects(x, y)), c(TRUE, FALSE))
  expect_equal(conv(ga_disjoint(x, y)), c(FALSE, TRUE))
})

test_that("intersects matches sf", {
  a <- sf::st_sfc(big(), big())
  b <- sf::st_sfc(small(), box(9, 9, 10, 10))

  expect_equal(
    conv(ga_intersects(ga(a), ga(b))),
    diag(sf::st_intersects(a, b, sparse = FALSE))
  )
})

test_that("touches is TRUE for shared edges only", {
  x <- ga(sf::st_sfc(box(0, 0, 1, 1), box(0, 0, 1, 1)))
  y <- ga(sf::st_sfc(box(1, 0, 2, 1), box(0.5, 0, 1.5, 1)))

  expect_equal(conv(ga_touches(x, y)), c(TRUE, FALSE))
})

test_that("overlaps needs partial intersection of equal ga_dimension", {
  x <- ga(sf::st_sfc(box(0, 0, 2, 2), big()))
  y <- ga(sf::st_sfc(box(1, 1, 3, 3), small()))

  expect_equal(conv(ga_overlaps(x, y)), c(TRUE, FALSE))
})

test_that("crosses is TRUE for a line through a polygon", {
  x <- ga(sf::st_sfc(sf::st_linestring(cbind(c(-1, 5), c(2, 2)))))
  y <- ga(sf::st_sfc(big()))

  expect_true(conv(ga_crosses(x, y)))
})

test_that("covers is weaker than contains on a boundary point", {
  x <- ga(sf::st_sfc(big()))
  edge <- ga(sf::st_sfc(pt(0, 2)))

  expect_true(conv(ga_covers(x, edge)))
  expect_false(conv(ga_contains(x, edge)))
})

test_that("covered_by mirrors ga_covers", {
  x <- ga(sf::st_sfc(big()))
  edge <- ga(sf::st_sfc(pt(0, 2)))

  expect_true(conv(ga_covered_by(edge, x)))
})

test_that("contains_properly is stricter than ga_contains", {
  x <- ga(sf::st_sfc(big(), big()))
  y <- ga(sf::st_sfc(small(), pt(0, 2)))

  expect_equal(conv(ga_contains_properly(x, y)), c(TRUE, FALSE))
})

test_that("equals_topo ignores winding and start point", {
  ring <- matrix(c(0, 0, 1, 0, 1, 1, 0, 1, 0, 0), ncol = 2, byrow = TRUE)
  reversed <- ring[rev(seq_len(nrow(ring))), ]

  x <- ga(sf::st_sfc(sf::st_polygon(list(ring))))
  y <- ga(sf::st_sfc(sf::st_polygon(list(reversed))))

  expect_true(conv(ga_equals_topo(x, y)))
})

test_that("predicates recycle y", {
  x <- ga(sf::st_sfc(big(), big(), big()))
  y <- ga(sf::st_sfc(small()))

  expect_equal(conv(ga_contains(x, y)), rep(TRUE, 3))
})

test_that("predicates reject a mismatched length", {
  x <- ga(sf::st_sfc(big(), big()))
  y <- ga(sf::st_sfc(small(), small(), small()))

  expect_error(ga_contains(x, y), "y")
})

test_that("a null geometry gives NA", {
  x <- ga(sf::st_sfc(big(), big()))
  y <- ga(sf::st_sfc(small(), sf::st_polygon()))
  res <- conv(ga_contains(x, y))

  expect_length(res, 2L)
  expect_true(res[1])
  expect_false(res[2])
})

test_that("relate returns a nine character DE-9IM string", {
  x <- ga(sf::st_sfc(big()))
  y <- ga(sf::st_sfc(small()))
  res <- conv(ga_relate(x, y))

  expect_length(res, 1L)
  expect_equal(nchar(res), 9L)
  expect_match(res, "^[F012]{9}$")
})

test_that("relate agrees with the named predicates", {
  x <- ga(sf::st_sfc(big()))
  y <- ga(sf::st_sfc(small()))

  expect_equal(conv(ga_relate(x, y)), "212FF1FF2")
  expect_true(conv(ga_contains(x, y)))
})

test_that("dimension reports 0, 1 and 2", {
  g <- ga(sf::st_sfc(
    pt(0, 0),
    sf::st_linestring(cbind(c(0, 1), c(0, 1))),
    big()
  ))

  expect_equal(conv(ga_dimension(g)), c(0L, 1L, 2L))
})

test_that("boundary_dimension is one lower for a polygon", {
  g <- ga(sf::st_sfc(big(), pt(0, 0)))
  res <- conv(ga_boundary_dimension(g))

  expect_equal(res[1], 1L)
  expect_true(is.na(res[2]))
})

test_that("dimension is NA for an empty geometry", {
  g <- ga(sf::st_sfc(big(), sf::st_polygon()))
  res <- conv(ga_dimension(g))

  expect_equal(res[1], 2L)
  expect_true(is.na(res[2]))
})

test_that("is_empty distinguishes empty from populated", {
  g <- ga(sf::st_sfc(big(), sf::st_polygon()))
  expect_equal(conv(ga_is_empty(g)), c(FALSE, TRUE))
})

test_that("coordinate_position separates inside, boundary and outside", {
  g <- ga(sf::st_sfc(big(), big(), big()))
  p <- ga(sf::st_sfc(pt(2, 2), pt(0, 2), pt(9, 9)))

  expect_equal(
    conv(ga_coordinate_position(g, p)),
    c("inside", "boundary", "outside")
  )
})

test_that("coordinate_position recycles a single point", {
  g <- ga(sf::st_sfc(big(), small()))
  p <- ga(sf::st_sfc(pt(1.5, 1.5)))

  expect_equal(conv(ga_coordinate_position(g, p)), c("inside", "inside"))
})

test_that("topology reads a concrete array from a reader", {
  skip_deps()
  path <- system.file("shape/nc.shp", package = "sf")
  skip_if(path == "")

  g <- as.data.frame(read_shapefile(path))$geometry
  p <- ga(sf::st_sfc(pt(-79, 35)))

  expect_length(conv(ga_intersects(g, p)), 100L)
  expect_length(conv(ga_dimension(g)), 100L)
  expect_true(any(conv(ga_coordinate_position(g, p)) == "outside"))
})
