source("bench/duckspatial/setup.R")

for (n in SIZES) {
  pts <- points_sf(n)

  bench(
    "dissolve",
    n,
    "geoarrowrs",
    {
      tbl <- as_arrow_table(st_drop_geometry(pts))
      tbl$geometry <- as_arrow_array(wk::as_wkb(st_geometry(pts)))$cast(
        arrow::binary()
      )
      groups <- tbl |> count(category) |> arrange(category) |> compute()
      rows <- tbl |> arrange(category) |> compute()
      out <- ga_collect_agg(
        as_nanoarrow_array(rows$geometry),
        sizes = as.vector(groups$n)
      )
    },
    out$length
  )

  if (HAS_DUCKSPATIAL) {
    bench(
      "dissolve",
      n,
      "duckspatial",
      out <- duckspatial::ddbs_union_agg(pts, by = "category", quiet = TRUE),
      ddbs_rows(out)
    )
  }

  if (HAS_SEDONADB) {
    bench(
      "dissolve",
      n,
      "sedonadb",
      {
        sd_to_view(as_sedonadb_dataframe(pts), "pts", overwrite = TRUE)
        out <- sd_compute(sd_sql(
          "SELECT category, ST_Collect_Agg(geometry) AS geometry
           FROM pts GROUP BY category"
        ))
      },
      sd_count(out)
    )
  }
}
