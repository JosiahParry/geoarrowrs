# Q12: pickups ranked by mean distance to their 5 nearest buildings.
#
# A nearest neighbour join. The R-tree answers all 6M queries in one call and
# returns five rows each, the distances are measured on the pairs the tree gave,
# and the per trip mean runs in Acero.

source("bench/setup.R")

b <- scan_cols("building", "b_boundary")
t <- scan_cols("trip", c("t_tripkey", "t_pickuploc"))

pickup <- ga_as_point(as_nanoarrow_array(t$t_pickuploc))
nearest <- RTree$new(ga_from_wkb(as_nanoarrow_array(b$b_boundary)))$neighbors(
  pickup,
  k = 5
)

trip_row <- left_rows(nearest)
building_row <- right_rows(nearest)

distance <- ga_dist_euclidean_pairwise(
  ga_as_point(as_nanoarrow_array(take(t$t_pickuploc, trip_row))),
  ga_from_wkb(as_nanoarrow_array(take(b$b_boundary, building_row)))
)

run("q12", arrow_table(t_tripkey = take(t$t_tripkey, trip_row)) |>
  with_column("distance", distance) |>
  group_by(t_tripkey) |>
  summarise(avg_distance_to_5_nearest = mean(distance)) |>
  arrange(desc(avg_distance_to_5_nearest), t_tripkey) |>
  head(100) |>
  collect())
