source("bench/duckspatial/setup.R")

polys <- polygons_sf()
polys_ga <- as_ga_frame(polys)

for (n in SIZES) {
  pts <- points_sf(n)

  bench(
    "join",
    n,
    "geoarrowrs",
    out <- ga_join(as_ga_frame(pts), polys_ga, ga_sparse_within, left = FALSE),
    out$num_rows
  )

  bench(
    "join",
    n,
    "sf",
    out <- st_join(pts, polys, join = st_within),
    nrow(out)
  )

  if (HAS_DUCKSPATIAL) {
    bench(
      "join",
      n,
      "duckspatial",
      out <- duckspatial::ddbs_join(pts, polys, join = "within", quiet = TRUE),
      ddbs_rows(out)
    )
  }
}
