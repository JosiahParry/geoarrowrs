# Dissolve: one geometry per category, from n points in four groups.
#
# This is the one operation the three do not answer alike. `ST_Union_Agg` and
# `sf::st_union()` both give a MULTIPOINT per group. `ga_collect_agg()` gives a
# GEOMETRYCOLLECTION: the same grouping and the same assembly of n points into
# four geometries, but not the same output type, because `ga_unary_union()`
# unions polygons only and returns an empty multipolygon for points. Treat the
# geoarrowrs column here as the cost of the grouped aggregate, not as the same
# call.
#
# The run lengths come from the ordering, which is what `ga_collect_agg()`
# takes: sort by the group, count each run, and the geometry follows.

source("bench/duckspatial/setup.R")

for (n in SIZES) {
  pts <- points_sf(n)

  out <- NULL
  t_ours <- elapsed({
    tbl <- as_arrow(pts)
    kernel <- elapsed({
      # the run lengths ga_collect_agg() takes are what the same ordering gives
      groups <- tbl |> count(category) |> arrange(category) |> compute()
      rows <- tbl |> arrange(category) |> compute()
      out <- ga_collect_agg(
        as_nanoarrow_array(rows$geometry),
        sizes = as.vector(groups$n)
      )
    })
  })
  report("dissolve", n, "geoarrowrs", t_ours, out$length, kernel)
  rm(out, tbl, rows)
  invisible(gc())

  out <- NULL
  t_sf <- elapsed({
    out <- pts |>
      dplyr::group_by(category) |>
      dplyr::summarise(geometry = sf::st_union(geometry))
  })
  report("dissolve", n, "sf", t_sf, nrow(out))
  rm(out)
  invisible(gc())

  if (HAS_DUCKSPATIAL) {
    out <- NULL
    t_ddbs <- elapsed(
      out <- duckspatial::ddbs_union_agg(pts, by = "category", quiet = TRUE)
    )
    report("dissolve", n, "duckspatial", t_ddbs, ddbs_rows(out))
    rm(out)
    invisible(gc())
  }
}
