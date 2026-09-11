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

  if (RUN_SF) {
    bench("distance", n, "sf", out <- st_distance(pts, pts), nrow(out))
  }

  if (HAS_DUCKSPATIAL) {
    bench(
      "distance",
      n,
      "duckspatial",
      out <- duckspatial::ddbs_distance(pts, pts, quiet = TRUE),
      ddbs_rows(out)
    )
  }

  if (HAS_SEDONADB) {
    bench(
      "distance",
      n,
      "sedonadb",
      {
        sd_to_view(as_sedonadb_dataframe(pts), "pts", overwrite = TRUE)
        out <- sd_compute(sd_sql(
          "SELECT ST_Distance(a.geometry, b.geometry) AS distance
           FROM pts a CROSS JOIN pts b"
        ))
      },
      sd_count(out)
    )
  }
}
