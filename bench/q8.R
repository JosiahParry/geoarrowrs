# Q8: pickups within 500m of each building.
#
# ST_DWithin against a polygon is an intersection with the buffered polygon, so
# the buildings are buffered once and the predicate does the rest. Counting the
# matches per building is a group_by in Acero.

source("bench/setup.R")

b <- scan_cols("building", c("b_buildingkey", "b_name"))
near <- ga_sparse_intersects(
  ga_buffer(geometry("building", "b_boundary"), 0.0045),
  geometry("trip", "t_pickuploc", ga_as_point)
)

building_row <- left_rows(near)

run("q8", arrow_table(
  b_buildingkey = take(b$b_buildingkey, building_row),
  b_name = take(b$b_name, building_row)
) |>
  count(b_buildingkey, b_name, name = "nearby_pickup_count") |>
  arrange(desc(nearby_pickup_count), b_buildingkey) |>
  head(100) |>
  collect())
