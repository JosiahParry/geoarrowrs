# Q4: zone distribution of the top 1000 trips by tip.
#
# The ranking is a plain Acero sort, so only 1000 rows leave the engine. Those
# pickups are matched to zones by a sparse predicate, and the zone attributes
# are pulled out by index with `take` rather than joined, so the counting is a
# straight group_by on an Arrow table.

source("bench/setup.R")

top <- trip |>
  arrange(desc(t_tip), t_tripkey) |>
  head(1000) |>
  select(t_pickuploc) |>
  compute()

hits <- ga_sparse_within(
  ga_as_point(as_nanoarrow_array(top$t_pickuploc)),
  geometry("zone", "z_boundary")
)
matched <- right_rows(hits)

run("q4", arrow_table(
  z_zonekey = take(zone$z_zonekey, matched),
  z_name = take(zone$z_name, matched)
) |>
  count(z_zonekey, z_name, name = "trip_count") |>
  arrange(desc(trip_count), z_zonekey) |>
  collect())
