# What a ddbs_*() call costs before it does any work.
#
# The published benchmark passes sf objects and no `conn`. The first such call
# opens an in-memory database and loads the spatial extension; the rest reuse
# it. The two rows here are that first call and a second identical one, so the
# fixed charge and what is left of it can be read off rather than guessed at.

source("bench/duckspatial/setup.R")

if (HAS_DUCKSPATIAL) {
  two <- points_sf(2)

  # the same call the other files make, on as little data as it will take
  out <- NULL
  first <- elapsed(out <- duckspatial::ddbs_intersects(two, two, quiet = TRUE))
  report("connect", 2, "duckspatial", first, ddbs_rows(out))

  out <- NULL
  again <- elapsed(out <- duckspatial::ddbs_intersects(two, two, quiet = TRUE))
  report("connect", 2, "duckspatial (second call)", again, ddbs_rows(out))
}
