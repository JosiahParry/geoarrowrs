# Q3: monthly trip statistics for a buffered box around Sedona city center.
#
# Acero takes no geometry constants, so the distance to the literal box is
# measured by the kernel and attached to the table as an ordinary column. The
# filter, the monthly rollup and the averages then all run in the engine.

source("bench/setup.R")

t0 <- Sys.time()

t <- scan_cols("trip", c(
  "t_tripkey", "t_pickuptime", "t_dropofftime", "t_distance", "t_fare"
))
t <- with_column(t, "distance_to_box", ga_dist_euclidean_pairwise(
  geometry("trip", "t_pickuploc"),
  literal(BOX_Q3_WKT)
))

out <- t |>
  filter(distance_to_box <= 0.045) |>
  mutate(
    pickup_month = strftime(
      utc_time(t_pickuptime),
      format = "%Y-%m-01",
      tz = "UTC"
    ),
    duration = (cast(t_dropofftime, int64()) - cast(t_pickuptime, int64())) /
      1000
  ) |>
  group_by(pickup_month) |>
  summarise(
    total_trips = n(),
    avg_distance = mean(cast(t_distance, float64())),
    avg_duration_seconds = mean(duration),
    avg_fare = mean(cast(t_fare, float64()))
  ) |>
  arrange(pickup_month) |>
  collect()

report("q3", t0, out)
