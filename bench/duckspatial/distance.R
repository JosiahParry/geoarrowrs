source("bench/duckspatial/setup.R")

for (n in c(1000, 5000, 10000)) {
  pts <- points_sf(n)
  geom <- geoarrow::as_geoarrow_array(st_geometry(pts))

  bench(
    "distance",
    n,
    "geoarrowrs",
    out <- ga_cross_distance(geom, geom, "haversine"),
    out$length
  )

  bench(
    "distance",
    n,
    "geoarrowrs (geodesic)",
    out <- ga_cross_distance(geom, geom, "geodesic"),
    out$length
  )

  bench("distance", n, "sf", out <- st_distance(pts, pts), nrow(out))

  if (HAS_DUCKSPATIAL) {
    bench(
      "distance",
      n,
      "duckspatial",
      out <- duckspatial::ddbs_distance(pts, pts, quiet = TRUE),
      ddbs_rows(out)
    )
  }
}
