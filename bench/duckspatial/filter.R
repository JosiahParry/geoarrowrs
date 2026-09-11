source("bench/duckspatial/setup.R")

polys <- polygons_sf()
polys_ga <- as_ga_frame(polys)

for (n in SIZES) {
  pts <- points_sf(n)

  bench(
    "filter",
    n,
    "geoarrowrs",
    out <- ga_filter(as_ga_frame(pts), polys_ga),
    out$num_rows
  )

  bench("filter", n, "sf", out <- st_filter(pts, polys), nrow(out))

  if (HAS_DUCKSPATIAL) {
    bench(
      "filter",
      n,
      "duckspatial",
      out <- duckspatial::ddbs_filter(pts, polys, quiet = TRUE),
      ddbs_rows(out)
    )
  }
}
