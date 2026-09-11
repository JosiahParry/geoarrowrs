source("bench/duckspatial/setup.R")

polys <- polygons_sf()
polys_ga <- geoarrow::as_geoarrow_array(st_geometry(polys))

for (n in SIZES) {
  pts <- points_sf(n)

  bench(
    "intersects",
    n,
    "geoarrowrs",
    out <- ga_sparse_intersects(
      geoarrow::as_geoarrow_array(st_geometry(pts)),
      polys_ga
    ),
    out$length
  )

  bench("intersects", n, "sf", out <- st_intersects(pts, polys), length(out))

  if (HAS_DUCKSPATIAL) {
    bench(
      "intersects",
      n,
      "duckspatial",
      out <- duckspatial::ddbs_intersects(pts, polys, quiet = TRUE),
      ddbs_rows(out)
    )
  }
}
