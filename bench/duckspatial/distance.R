# Cross distance: every point against every point.
#
# For EPSG:4326 points `ddbs_distance()` defaults to `dist_type = "haversine"`,
# which is DuckDB's `ST_Distance_Sphere`, so that is what is measured. sf on
# geographic coordinates measures with s2, a slightly different sphere, so the
# three agree to a relative part in ten thousand rather than exactly.
#
# n against n means n^2 distances: ten thousand points is a hundred million.

source("bench/duckspatial/setup.R")

DIST_SIZES <- c(1000, 5000, 10000)

for (n in DIST_SIZES) {
  pts <- points_sf(n)

  out <- NULL
  t_ours <- elapsed({
    geom <- geometry(pts)
    kernel <- elapsed(out <- ga_cross_distance(geom, geom, "haversine"))
  })
  report("distance", n, "geoarrowrs", t_ours, out$length, kernel)
  mine <- as.vector(out)[[1]]
  rm(out)
  invisible(gc())

  # the same work on the ellipsoid rather than a sphere, which is the answer
  # duckspatial could have given here and did not: its default for EPSG:4326
  # points is ST_Distance_Sphere, and a sphere is off by up to about 0.5%
  out <- NULL
  t_exact <- elapsed({
    geom <- geometry(pts)
    kernel <- elapsed(out <- ga_cross_distance(geom, geom, "geodesic"))
  })
  report("distance", n, "geoarrowrs (geodesic)", t_exact, out$length, kernel)
  rm(out)
  invisible(gc())

  out <- NULL
  t_sf <- elapsed(out <- sf::st_distance(pts, pts))
  report("distance", n, "sf", t_sf, nrow(out))
  if (n == DIST_SIZES[1]) {
    stopifnot(all.equal(mine, as.numeric(out[1, ]), tolerance = 1e-4))
    cat("agrees with sf::st_distance()\n")
  }
  rm(out)
  invisible(gc())

  if (HAS_DUCKSPATIAL) {
    out <- NULL
    t_ddbs <- elapsed(out <- duckspatial::ddbs_distance(pts, pts, quiet = TRUE))
    report("distance", n, "duckspatial", t_ddbs, ddbs_rows(out))
    rm(out)
    invisible(gc())
  }
}
