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

  if (HAS_DUCKSPATIAL) {
    bench(
      "filter",
      n,
      "duckspatial",
      out <- duckspatial::ddbs_filter(pts, polys, quiet = TRUE),
      ddbs_rows(out)
    )
  }

  if (HAS_SEDONADB) {
    bench(
      "filter",
      n,
      "sedonadb",
      {
        sd_to_view(as_sedonadb_dataframe(pts), "pts", overwrite = TRUE)
        sd_to_view(as_sedonadb_dataframe(polys), "polys", overwrite = TRUE)
        out <- sd_compute(sd_sql(
          "SELECT a.* FROM pts a
           WHERE EXISTS (
             SELECT 1 FROM polys b WHERE ST_Intersects(a.geometry, b.geometry)
           )"
        ))
      },
      sd_count(out)
    )
  }
}
