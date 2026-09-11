# Q7: route detours, reported distance against the straight line between ends.
#
# Entirely in Acero. ST_Length(ST_MakeLine(a, b)) is the distance from a to b,
# so this is one kernel rather than a construction and a measurement.

source("bench/setup.R")

t0 <- Sys.time()

out <- dataset("trip") |>
  mutate(
    reported_distance_m = t_distance,
    # the benchmark converts degrees to metres with a flat factor
    line_distance_m = ga_dist_euclidean_pairwise(t_pickuploc, t_dropoffloc) /
      0.000009
  ) |>
  mutate(detour_ratio = reported_distance_m / line_distance_m) |>
  select(t_tripkey, reported_distance_m, line_distance_m, detour_ratio) |>
  arrange(desc(detour_ratio), desc(reported_distance_m), t_tripkey) |>
  head(100) |>
  collect()

report("q7", t0, out)
