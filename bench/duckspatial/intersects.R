# Geometry predicate: which polygons each point intersects.
#
# `sf::st_intersects()` and `ga_sparse_intersects()` both return the matching
# rows of y per row of x. `ddbs_intersects()` returns the pairs as a table, so
# its row count is the number of matches rather than the number of points.

source("bench/duckspatial/setup.R")

polys <- polygons_sf()
polys_ga <- geometry(polys)

for (n in SIZES) {
  pts <- points_sf(n)

  out <- NULL
  t_ours <- elapsed({
    pts_ga <- geometry(pts)
    kernel <- elapsed(out <- ga_sparse_intersects(pts_ga, polys_ga))
  })
  report("intersects", n, "geoarrowrs", t_ours, out$length, kernel)
  rm(out)
  invisible(gc())

  out <- NULL
  t_sf <- elapsed(out <- sf::st_intersects(pts, polys))
  report("intersects", n, "sf", t_sf, length(out))
  rm(out)
  invisible(gc())

  if (HAS_DUCKSPATIAL) {
    out <- NULL
    t_ddbs <- elapsed(out <- duckspatial::ddbs_intersects(pts, polys, quiet = TRUE))
    report("intersects", n, "duckspatial", t_ddbs, ddbs_rows(out))
    rm(out)
    invisible(gc())
  }
}
