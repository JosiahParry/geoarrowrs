# Spatial join: attach the polygon attributes to each point inside it.
#
# The three do not agree on what a spatial join returns. `ddbs_join()` is a SQL
# inner join, `sf::st_join()` is a left join, so sf keeps the points that fall
# in no polygon and duckspatial drops them. `ga_join(left = FALSE)` matches
# duckspatial. The row counts are reported so the difference is visible rather
# than hidden in a ratio.

source("bench/duckspatial/setup.R")

polys <- polygons_sf()
polys_ga <- as_ga_frame(polys)

for (n in SIZES) {
  pts <- points_sf(n)

  out <- NULL
  t_ours <- elapsed({
    pts_ga <- as_ga_frame(pts)
    kernel <- elapsed(
      out <- ga_join(pts_ga, polys_ga, ga_sparse_within, left = FALSE)
    )
  })
  report("join", n, "geoarrowrs", t_ours, out$num_rows, kernel)
  rm(out)
  invisible(gc())

  out <- NULL
  t_sf <- elapsed(out <- sf::st_join(pts, polys, join = sf::st_within))
  report("join", n, "sf", t_sf, nrow(out))
  rm(out)
  invisible(gc())

  if (HAS_DUCKSPATIAL) {
    out <- NULL
    t_ddbs <- elapsed(
      out <- duckspatial::ddbs_join(pts, polys, join = "within", quiet = TRUE)
    )
    report("join", n, "duckspatial", t_ddbs, ddbs_rows(out))
    rm(out)
    invisible(gc())
  }
}
