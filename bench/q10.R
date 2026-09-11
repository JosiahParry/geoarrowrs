# Q10: zone statistics for the trips starting inside each zone.
#
# A left join: every zone appears, including those no trip starts in. The
# spatial match gives the pairs, Acero aggregates them, and a left_join back
# onto the zone table restores the zones with no trips.

source("bench/setup.R")

t0 <- Sys.time()

zone <- scan_cols("zone", c("z_zonekey", "z_name"))
hits <- ga_sparse_contains(
  geometry("zone", "z_boundary"),
  geometry("trip", "t_pickuploc")
)

t <- scan_cols("trip", c("t_pickuptime", "t_dropofftime", "t_distance"))
zone_row <- left_rows(hits)
trip_row <- right_rows(hits)

stats <- arrow_table(
  z_zonekey = take(zone$z_zonekey, zone_row),
  distance = take(t$t_distance, trip_row),
  pickup = take(t$t_pickuptime, trip_row),
  dropoff = take(t$t_dropofftime, trip_row)
) |>
  mutate(
    duration = (cast(dropoff, int64()) - cast(pickup, int64())) / 1000
  ) |>
  group_by(z_zonekey) |>
  summarise(
    avg_duration_seconds = mean(duration),
    avg_distance = mean(cast(distance, float64())),
    num_trips = n()
  ) |>
  compute()

out <- zone |>
  left_join(stats, by = "z_zonekey") |>
  mutate(
    pickup_zone = z_name,
    num_trips = coalesce(num_trips, 0L)
  ) |>
  select(
    z_zonekey,
    pickup_zone,
    avg_duration_seconds,
    avg_distance,
    num_trips
  ) |>
  arrange(is.na(avg_duration_seconds), desc(avg_duration_seconds), z_zonekey) |>
  head(100) |>
  collect()

report("q10", t0, out)
