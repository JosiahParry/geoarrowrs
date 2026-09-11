library(arrow)
library(dplyr)
library(nanoarrow)
library(sf)
library(geoarrowrs)

SIZES <- as.numeric(strsplit(
  Sys.getenv("DUCKSPATIAL_BENCH_SIZES", "1e5,1e6,3e6"),
  ",",
  fixed = TRUE
)[[1]])
SEED <- 27
CACHE <- Sys.getenv(
  "DUCKSPATIAL_BENCH_DATA",
  tools::R_user_dir("geoarrowrs", "cache")
)

HAS_DUCKSPATIAL <- requireNamespace("duckspatial", quietly = TRUE)
HAS_SEDONADB <- requireNamespace("sedonadb", quietly = TRUE)
RUN_SF <- nzchar(Sys.getenv("DUCKSPATIAL_BENCH_SF"))

if (HAS_SEDONADB) {
  library(sedonadb)
}

make_points <- function(n) {
  data.frame(
    id = 1:n,
    x = runif(n, min = -180, max = 180),
    y = runif(n, min = -90, max = 90),
    value = rnorm(n, mean = 100, sd = 15),
    category = sample(c("A", "B", "C", "D"), n, replace = TRUE)
  ) |>
    st_as_sf(coords = c("x", "y"), crs = 4326)
}

make_polygons <- function(n = 10000) {
  polys <- vector("list", n)
  for (i in seq_len(n)) {
    cx <- runif(1, min = -170, max = 170)
    cy <- runif(1, min = -80, max = 80)
    w <- runif(1, min = 0.5, max = 3)
    h <- runif(1, min = 0.5, max = 3)
    polys[[i]] <- st_polygon(list(cbind(
      c(cx - w / 2, cx + w / 2, cx + w / 2, cx - w / 2, cx - w / 2),
      c(cy - h / 2, cy - h / 2, cy + h / 2, cy + h / 2, cy - h / 2)
    )))
  }

  st_sf(
    poly_id = seq_len(n),
    region = sample(c("North", "South", "East", "West"), n, replace = TRUE),
    population = sample(1000:1000000, n, replace = TRUE),
    geometry = st_sfc(polys, crs = 4326)
  )
}

cached <- function(name, make) {
  dir.create(CACHE, showWarnings = FALSE, recursive = TRUE)
  path <- file.path(CACHE, paste0(name, ".rds"))
  if (!file.exists(path)) {
    saveRDS(withr::with_seed(SEED, make()), path)
  }
  readRDS(path)
}

points_sf <- function(n) {
  cached(paste0("points-", format(n, scientific = FALSE)), \() make_points(n))
}

polygons_sf <- function() cached("polygons", make_polygons)

as_ga_frame <- function(x) {
  out <- st_drop_geometry(x)
  out$geometry <- geoarrow::as_geoarrow_vctr(st_geometry(x))
  out
}

ddbs_rows <- function(x) pull(count(x), n)

bench <- function(op, n, pkg, expr, rows) {
  t0 <- Sys.time()
  force(expr)
  seconds <- as.numeric(Sys.time() - t0, units = "secs")
  rows <- force(rows)

  cat(sprintf(
    "%-10s n = %-9s %-22s %7.2fs  %s rows\n",
    op,
    format(n, scientific = FALSE),
    pkg,
    seconds,
    format(rows, big.mark = ",")
  ))

  log <- Sys.getenv("DUCKSPATIAL_BENCH_LOG", "")
  if (nzchar(log)) {
    started <- file.exists(log)
    write.table(
      data.frame(op, n = as.integer(n), pkg, seconds = round(seconds, 3), rows),
      log,
      append = started,
      col.names = !started,
      row.names = FALSE,
      sep = ",",
      qmethod = "double"
    )
  }

  invisible(gc())
}
