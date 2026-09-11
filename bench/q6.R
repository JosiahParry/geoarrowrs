# Q6: zone statistics for trips inside zones that meet a bounding box.
#
# Two spatial steps, both sparse predicates: the zones the literal box meets,
# then the pickups those zones contain. Zone attributes come out by index with
# `take`, the trip measures by index too, and the rollup runs in Acero.

source("bench/setup.R")

boundaries <- geometry("zone", "z_boundary")
in_box <- right_rows(ga_sparse_intersects(literal(BOX_Q6_WKT), boundaries))

wanted <- as_geoarrow_vctr(boundaries)[as.vector(in_box)]
hits <- ga_sparse_contains(
  wanted,
  geometry("trip", "t_pickuploc", ga_as_point)
)

t <- scan_cols("trip", c("t_pickuptime", "t_dropofftime", "t_distance"))
zone_row <- take(in_box, left_rows(hits))
trip_row <- right_rows(hits)

run("q6", arrow_table(
  z_zonekey = take(zone$z_zonekey, zone_row),
  z_name = take(zone$z_name, zone_row),
  distance = take(t$t_distance, trip_row),
  pickup = take(t$t_pickuptime, trip_row),
  dropoff = take(t$t_dropofftime, trip_row)
) |>
  mutate(
    duration = (cast(dropoff, int64()) - cast(pickup, int64())) / 1000
  ) |>
  group_by(z_zonekey, z_name) |>
  summarise(
    total_pickups = n(),
    avg_distance = mean(cast(distance, float64())),
    avg_duration_seconds = mean(duration)
  ) |>
  arrange(desc(total_pickups), z_zonekey) |>
  collect())
