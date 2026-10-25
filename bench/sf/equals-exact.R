# st_equals_exact() from mask_sf() against sf's own, which asks GEOS.
#
# Run from the repository root with a release build installed. A debug build,
# which `devtools::load_all()` gives, is several times slower (see
# bench/setup.R):
#
#   env -u DEBUG NOT_CRAN=true R CMD INSTALL --no-multiarch .
#   Rscript bench/sf/equals-exact.R
#
# Each row of `x` matches two rows of `y`: its own copy, and a copy moved by
# less than the tolerance. sf compares every row of `x` with every row of `y`,
# so its time grows with the square of the size, and the script times sf only
# up to EQUALS_EXACT_BENCH_SF_MAX rows. Where both run, the script checks that
# they return the same matches.

source("bench/duckspatial/setup.R")
mask_sf()

TOL <- 1e-6
SHIFT <- 1e-7
sizes <- function(var, default) {
  as.numeric(strsplit(Sys.getenv(var, default), ",", fixed = TRUE)[[1]])
}
POINT_SIZES <- sizes("EQUALS_EXACT_BENCH_SIZES", "1e3,1e4,1e5,1e6")
POLYGON_SIZES <- sizes("EQUALS_EXACT_BENCH_POLYGON_SIZES", "1e3,1e4")
SF_MAX <- as.numeric(Sys.getenv("EQUALS_EXACT_BENCH_SF_MAX", "1e4"))

rows_of <- function(hits) lapply(hits, as.integer)

compare <- function(op, x, y) {
  n <- length(x)
  bench(
    op,
    n,
    "geoarrowrs",
    ours <- st_equals_exact(x, y, par = TOL),
    sum(lengths(as.vector(ours)))
  )

  if (n <= SF_MAX) {
    x_sf <- sf::st_as_sfc(x)
    y_sf <- sf::st_as_sfc(y)
    bench(
      op,
      n,
      "sf",
      theirs <- sf::st_equals_exact(x_sf, y_sf, par = TOL),
      sum(lengths(theirs))
    )
    stopifnot(identical(rows_of(as.vector(ours)), rows_of(theirs)))
  }
}

for (n in POINT_SIZES) {
  withr::with_seed(SEED, {
    px <- runif(n, min = -180, max = 180)
    py <- runif(n, min = -90, max = 90)
  })
  compare(
    "equals_exact points",
    geoarrow::as_geoarrow_vctr(ga_point(px, py)),
    geoarrow::as_geoarrow_vctr(ga_point(c(px, px + SHIFT), c(py, py)))
  )
}

# planar coordinates, since st_equals_exact() measures planar distance
polys <- st_set_crs(st_geometry(polygons_sf()), NA)
for (n in POLYGON_SIZES) {
  x <- polys[seq_len(n)]
  compare(
    "equals_exact polygons",
    geoarrow::as_geoarrow_vctr(x),
    geoarrow::as_geoarrow_vctr(c(x, x + c(SHIFT, 0)))
  )
}
