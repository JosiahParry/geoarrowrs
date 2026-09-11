# Spatial filter: keep the points that fall in any polygon.
#
# The answer is the surviving rows of x, so all three return the same thing and
# the row counts should agree. sf disagrees by a few rows because it measures
# with s2, which treats a polygon edge as a geodesic rather than as a straight
# line in longitude and latitude, so points near an edge fall the other way.

source("bench/duckspatial/setup.R")

polys <- polygons_sf()
polys_ga <- as_ga_frame(polys)

for (n in SIZES) {
  pts <- points_sf(n)

  out <- NULL
  t_ours <- elapsed({
    pts_ga <- as_ga_frame(pts)
    kernel <- elapsed(out <- ga_filter(pts_ga, polys_ga))
  })
  report("filter", n, "geoarrowrs", t_ours, out$num_rows, kernel)
  rm(out)
  invisible(gc())

  out <- NULL
  t_sf <- elapsed(out <- sf::st_filter(pts, polys))
  report("filter", n, "sf", t_sf, nrow(out))
  rm(out)
  invisible(gc())

  if (HAS_DUCKSPATIAL) {
    out <- NULL
    t_ddbs <- elapsed(out <- duckspatial::ddbs_filter(pts, polys, quiet = TRUE))
    report("filter", n, "duckspatial", t_ddbs, ddbs_rows(out))
    rm(out)
    invisible(gc())
  }
}
