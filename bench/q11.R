# Q11: trips whose pickup and dropoff fall in different zones.
#
# Two point in polygon matches over the same zones. Each gives a trip row and
# the zone it landed in; joining them on the trip row and comparing the two zone
# keys is ordinary relational work, so all of it runs in Acero.

source("bench/setup.R")

t0 <- Sys.time()

zone <- scan_cols("zone", c("z_zonekey", "z_name"))
boundaries <- geometry("zone", "z_boundary")

from <- ga_sparse_contains(
  boundaries,
  geometry("trip", "t_pickuploc")
)
to <- ga_sparse_contains(
  boundaries,
  geometry("trip", "t_dropoffloc")
)

pickups <- arrow_table(
  trip_row = right_rows(from),
  pickup_zone = take(zone$z_zonekey, left_rows(from))
)
dropoffs <- arrow_table(
  trip_row = right_rows(to),
  dropoff_zone = take(zone$z_zonekey, left_rows(to))
)

out <- pickups |>
  inner_join(dropoffs, by = "trip_row") |>
  filter(pickup_zone != dropoff_zone) |>
  summarise(cross_zone_trip_count = n()) |>
  collect()

report("q11", t0, out)
