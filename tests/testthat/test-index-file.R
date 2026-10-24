skip_deps <- function() {
  skip_if_not_installed("sf")
  skip_if_not_installed("geoarrow")
  skip_if_not_installed("withr")
}

nc <- function() {
  skip_deps()
  as.data.frame(
    read_shapefile(system.file("shape/nc.shp", package = "sf"))
  )$geometry
}

test_that("an RTree answers the same queries after a write and read", {
  g <- nc()
  idx <- RTree$new(g)
  path <- withr::local_tempfile(fileext = ".gaix")
  idx$write(path)

  for (mmap in c(FALSE, TRUE)) {
    saved <- RTree$read(path, mmap = mmap)

    expect_equal(saved$size(), idx$size())
    expect_equal(saved$n_indexed(), idx$n_indexed())
    expect_equal(as.vector(saved$search(g)), as.vector(idx$search(g)))
    expect_equal(
      as.vector(saved$neighbors(ga_centroid(g), k = 3)),
      as.vector(idx$neighbors(ga_centroid(g), k = 3))
    )
  }
})

test_that("a KDTree answers the same queries after a write and read", {
  pts <- ga_centroid(nc())
  idx <- KDTree$new(pts)
  path <- withr::local_tempfile(fileext = ".gaix")
  idx$write(path)

  for (mmap in c(FALSE, TRUE)) {
    saved <- KDTree$read(path, mmap = mmap)

    expect_equal(saved$size(), idx$size())
    expect_equal(saved$n_indexed(), idx$n_indexed())
    expect_equal(
      as.vector(saved$within(pts, 0.5)),
      as.vector(idx$within(pts, 0.5))
    )
    expect_equal(
      as.vector(saved$range(ga_envelope(nc()))),
      as.vector(idx$range(ga_envelope(nc())))
    )
  }
})

test_that("a read index still returns the original rows around null rows", {
  skip_deps()
  g <- geoarrow::as_geoarrow_array(
    wk::wkt(c("POINT (0 0)", NA, "POINT (5 5)"))
  )
  path <- withr::local_tempfile(fileext = ".gaix")
  RTree$new(g)$write(path)
  saved <- RTree$read(path, mmap = TRUE)

  expect_equal(saved$size(), 3L)
  expect_equal(saved$n_indexed(), 2L)
  expect_equal(as.vector(saved$search(ga_xy(5, 5)))[[1]], 3L)
})

test_that("the file keeps the tree in flatbush layout on an 8 byte boundary", {
  g <- nc()
  path <- withr::local_tempfile(fileext = ".gaix")
  RTree$new(g)$write(path)
  bytes <- readBin(path, "raw", file.size(path))

  expect_identical(rawToChar(bytes[1:4]), "GAIX")
  tree_start <- ceiling((24 + 4 * length(g)) / 8) * 8
  # flatbush marks its buffers with the magic byte 0xfb
  expect_identical(bytes[tree_start + 1], as.raw(0xfb))
})

test_that("read rejects a file that is not an index", {
  skip_deps()
  path <- withr::local_tempfile(fileext = ".gaix")
  writeLines("not an index", path)

  expect_error(RTree$read(path), "Not a geoarrowrs index")
  expect_error(KDTree$read(path, mmap = TRUE), "Not a geoarrowrs index")
})

test_that("read rejects the other kind of tree", {
  g <- nc()
  rtree_path <- withr::local_tempfile(fileext = ".gaix")
  kdtree_path <- withr::local_tempfile(fileext = ".gaix")
  RTree$new(g)$write(rtree_path)
  KDTree$new(ga_centroid(g))$write(kdtree_path)

  expect_error(KDTree$read(rtree_path), "k-d tree")
  expect_error(RTree$read(kdtree_path), "R-tree")
})

test_that("read rejects a truncated file", {
  g <- nc()
  path <- withr::local_tempfile(fileext = ".gaix")
  RTree$new(g)$write(path)
  writeBin(readBin(path, "raw", 40), path)

  expect_error(RTree$read(path), "truncated")
})
