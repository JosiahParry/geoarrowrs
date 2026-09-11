source("bench/duckspatial/setup.R")

if (HAS_DUCKSPATIAL) {
  two <- points_sf(2)

  bench(
    "connect",
    2,
    "duckspatial",
    out <- duckspatial::ddbs_intersects(two, two, quiet = TRUE),
    ddbs_rows(out)
  )

  bench(
    "connect",
    2,
    "duckspatial (second call)",
    out <- duckspatial::ddbs_intersects(two, two, quiet = TRUE),
    ddbs_rows(out)
  )
}
