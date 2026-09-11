# Q1: trips starting within 50km of Sedona city center, ordered by distance.
#
# Entirely in Acero. Acero takes no geometry constants, so the distance to a
# fixed point is written as arithmetic on the ordinates rather than as a
# distance kernel against a literal. Only the 100 surviving rows reach R.

source("bench/setup.R")

SEDONA <- c(-111.7610, 34.8697)

t0 <- Sys.time()

out <- dataset("trip") |>
  mutate(
    pickup_lon = ga_x(t_pickuploc),
    pickup_lat = ga_y(t_pickuploc)
  ) |>
  mutate(
    distance_to_center = sqrt(
      (pickup_lon - SEDONA[1])^2 + (pickup_lat - SEDONA[2])^2
    )
  ) |>
  filter(distance_to_center <= 0.45) |>
  mutate(
    t_pickuptime = strftime(
      utc_time(t_pickuptime),
      format = "%Y-%m-%d %H:%M:%S",
      tz = "UTC"
    )
  ) |>
  select(
    t_tripkey, pickup_lon, pickup_lat, t_pickuptime, distance_to_center
  ) |>
  arrange(distance_to_center, t_tripkey) |>
  head(100) |>
  collect()

report("q1", t0, out)
