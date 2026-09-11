# Q4: zone distribution of the top 1000 trips by tip.
#
# The ranking is a plain Acero sort, so only 1000 rows leave the engine. Those
# pickups are matched to zones by a sparse predicate, and the zone attributes
# are pulled out by index with `take` rather than joined, so the counting is a
# straight group_by on an Arrow table.

source("bench/setup.R")

t0 <- Sys.time()

top <- dataset("trip") |>
  arrange(desc(t_tip), t_tripkey) |>
  head(1000) |>
  select(t_pickuploc) |>
  compute()

zone <- scan_cols("zone", c("z_zonekey", "z_name"))
hits <- ga_sparse_within(
  as_nanoarrow_array(top$t_pickuploc),
  geometry("zone", "z_boundary")
)
matched <- right_rows(hits)

out <- arrow_table(
  z_zonekey = take(zone$z_zonekey, matched),
  z_name = take(zone$z_name, matched)
) |>
  count(z_zonekey, z_name, name = "trip_count") |>
  arrange(desc(trip_count), z_zonekey) |>
  collect()

report("q4", t0, out)
