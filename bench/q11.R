# Q11: trips whose pickup and dropoff fall in different zones.
#
# Two point in polygon matches over the same zones. Each gives a trip row and
# the zone it landed in; joining them on the trip row and comparing the two zone
# keys is ordinary relational work, so all of it runs in Acero.

source("bench/setup.R")

boundaries <- geometry("zone", "z_boundary")

from <- ga_sparse_contains(
  boundaries,
  geometry("trip", "t_pickuploc", ga_as_point)
)
to <- ga_sparse_contains(
  boundaries,
  geometry("trip", "t_dropoffloc", ga_as_point)
)

pickups <- arrow_table(
  trip_row = right_rows(from),
  pickup_zone = take(zone$z_zonekey, left_rows(from))
)
dropoffs <- arrow_table(
  trip_row = right_rows(to),
  dropoff_zone = take(zone$z_zonekey, left_rows(to))
)

run(
  "q11",
  pickups |>
    inner_join(dropoffs, by = "trip_row") |>
    filter(pickup_zone != dropoff_zone) |>
    summarise(cross_zone_trip_count = n()) |>
    collect()
)

devtools::load_all("~/github/arcgislayers")

furl <- "https://mapprod3.environment.nsw.gov.au/arcgis/rest/services/Planning/EPI_Primary_Planning_Layers/MapServer/2"

flayer <- arc_open(furl)

res <- arc_count(
  flayer,
  fields = "LAY_CLASS",
  returnDistinctValues = "true"
)
