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

  if (HAS_DUCKSPATIAL) {
    bench(
      "join",
      n,
      "duckspatial",
      out <- duckspatial::ddbs_join(pts, polys, join = "within", quiet = TRUE),
      ddbs_rows(out)
    )
  }

  if (HAS_SEDONADB) {
    bench(
      "join",
      n,
      "sedonadb",
      {
        sd_to_view(as_sedonadb_dataframe(pts), "pts", overwrite = TRUE)
        sd_to_view(as_sedonadb_dataframe(polys), "polys", overwrite = TRUE)
        out <- sd_compute(sd_sql(
          "SELECT a.*, b.poly_id, b.region, b.population
           FROM pts a JOIN polys b ON ST_Within(a.geometry, b.geometry)"
        ))
      },
      sd_count(out)
    )
  }
}
