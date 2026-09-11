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

  if (RUN_SF) {
    bench("intersects", n, "sf", out <- st_intersects(pts, polys), length(out))
  }

  if (HAS_DUCKSPATIAL) {
    bench(
      "intersects",
      n,
      "duckspatial",
      out <- duckspatial::ddbs_intersects(pts, polys, quiet = TRUE),
      ddbs_rows(out)
    )
  }

  if (HAS_SEDONADB) {
    bench(
      "intersects",
      n,
      "sedonadb",
      {
        sd_to_view(as_sedonadb_dataframe(pts), "pts", overwrite = TRUE)
        sd_to_view(as_sedonadb_dataframe(polys), "polys", overwrite = TRUE)
        out <- sd_compute(sd_sql(
          "SELECT a.id, b.poly_id
           FROM pts a JOIN polys b ON ST_Intersects(a.geometry, b.geometry)"
        ))
      },
      sd_count(out)
    )
  }
}
