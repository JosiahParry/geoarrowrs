# Q12: pickups ranked by mean distance to their 5 nearest buildings.
#
# A nearest neighbour join. `ga_sparse_knn()` answers all 6M queries in one
# call and returns five rows each with the distance already measured, so there
# is no second pass over the geometry; the per trip mean runs in Acero.

source("bench/setup.R")

t0 <- Sys.time()

b <- scan_cols("building", "b_boundary")
t <- scan_cols("trip", c("t_tripkey", "t_pickuploc"))

nearest <- ga_sparse_knn(
  as_nanoarrow_array(t$t_pickuploc),
  as_nanoarrow_array(b$b_boundary),
  k = 5
)

out <- arrow_table(
  t_tripkey = take(t$t_tripkey, left_rows(nearest)),
  distance = knn_pairs(nearest)$GetFieldByName("distance")
) |>
  group_by(t_tripkey) |>
  summarise(avg_distance_to_5_nearest = mean(distance)) |>
  arrange(desc(avg_distance_to_5_nearest), t_tripkey) |>
  head(100) |>
  collect()

report("q12", t0, out)
