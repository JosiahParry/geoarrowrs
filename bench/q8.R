# Q8: pickups within 500m of each building.
#
# `ga_sparse_dwithin()` is ST_DWithin: it grows each building's box by the
# radius, then measures the candidates exactly. Buffering and intersecting
# instead would undercount, because a buffer approximates its arcs with
# segments. Counting the matches per building is a group_by in Acero.

source("bench/setup.R")

t0 <- Sys.time()

b <- scan_cols("building", c("b_buildingkey", "b_name"))
near <- ga_sparse_dwithin(
  geometry("building", "b_boundary"),
  geometry("trip", "t_pickuploc"),
  0.0045
)

building_row <- left_rows(near)

out <- arrow_table(
  b_buildingkey = take(b$b_buildingkey, building_row),
  b_name = take(b$b_name, building_row)
) |>
  count(b_buildingkey, b_name, name = "nearby_pickup_count") |>
  arrange(desc(nearby_pickup_count), b_buildingkey) |>
  head(100) |>
  collect()

report("q8", t0, out)
