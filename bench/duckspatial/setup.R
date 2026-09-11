# Helpers for the duckspatial comparison.
#
# The operations, the data, and the calls come from
# https://cidree.github.io/duckspatial/articles/benchmark.html so the numbers
# published there mean the same thing as the ones here. Each operation is its
# own file, generates its own data, and times itself, run from the repository
# root:
#
#   env -u DEBUG Rscript bench/duckspatial/join.R
#
# Install a release build first. `devtools::load_all()` and `rextendr::document()`
# set `DEBUG`, which `tools/config.R` turns into a cargo debug build:
#
#   env -u DEBUG NOT_CRAN=true R CMD INSTALL --no-multiarch .
#
# Every implementation starts from the same in-memory `sf` objects and is timed
# until its answer exists, whole, wherever that package keeps answers: an R
# object for sf, an Arrow array for geoarrowrs, a DuckDB temp table for
# duckspatial, which is what `ddbs_*()` leaves behind before handing back a lazy
# `tbl` over it. None of the three is timed reading the answer back into R.
#
# So the sf to GeoArrow and sf to DuckDB conversions are both inside the
# timings, because both are work a caller cannot avoid. The `kernel` column
# reports how much of our own time was spent past that conversion.
#
# No `conn` is passed to any `ddbs_*()` call, which is how the published
# benchmark calls them. The first such call in a session opens an in-memory
# database and loads the spatial extension, and the rest reuse it, so the
# charge lands once on whichever call comes first rather than on every one.
# `bench/duckspatial/connect.R` measures it, since at the small sizes it is
# most of what the duckspatial column reports.

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
N_POLYGONS <- 10000
SEED <- 27

HAS_DUCKSPATIAL <- requireNamespace("duckspatial", quietly = TRUE)

CACHE <- Sys.getenv(
  "DUCKSPATIAL_BENCH_DATA",
  tools::R_user_dir("geoarrowrs", "cache")
)

#' The points duckspatial generates, from its `make_points()`
make_points <- function(n_points) {
  data.frame(
    id = 1:n_points,
    x = runif(n_points, min = -180, max = 180),
    y = runif(n_points, min = -90, max = 90),
    value = rnorm(n_points, mean = 100, sd = 15),
    category = sample(c("A", "B", "C", "D"), n_points, replace = TRUE)
  ) |>
    sf::st_as_sf(coords = c("x", "y"), crs = 4326)
}

#' The 10,000 rectangles duckspatial generates
#'
#' The published article draws these outside its `withr::with_seed()`, so they
#' depend on whatever the session had already drawn and are not reproducible
#' from the code as written. Seeded here so that they are.
make_polygons <- function(n_polygons = N_POLYGONS) {
  polygons_list <- vector("list", n_polygons)
  for (i in 1:n_polygons) {
    center_x <- runif(1, min = -170, max = 170)
    center_y <- runif(1, min = -80, max = 80)
    width <- runif(1, min = 0.5, max = 3)
    height <- runif(1, min = 0.5, max = 3)

    x_coords <- c(
      center_x - width / 2,
      center_x + width / 2,
      center_x + width / 2,
      center_x - width / 2,
      center_x - width / 2
    )
    y_coords <- c(
      center_y - height / 2,
      center_y - height / 2,
      center_y + height / 2,
      center_y + height / 2,
      center_y - height / 2
    )

    polygons_list[[i]] <- sf::st_polygon(list(cbind(x_coords, y_coords)))
  }

  sf::st_sf(
    poly_id = 1:n_polygons,
    region = sample(c("North", "South", "East", "West"), n_polygons, replace = TRUE),
    population = sample(1000:1000000, n_polygons, replace = TRUE),
    geometry = sf::st_sfc(polygons_list, crs = 4326)
  )
}

#' Generate a dataset once and keep it, since five files want the same one
cached <- function(name, make) {
  dir.create(CACHE, showWarnings = FALSE, recursive = TRUE)
  path <- file.path(CACHE, paste0(name, ".rds"))
  if (!file.exists(path)) {
    saveRDS(withr::with_seed(SEED, make()), path)
  }
  readRDS(path)
}

points_sf <- function(n) cached(paste0("points-", format(n, scientific = FALSE)), function() make_points(n))

polygons_sf <- function() cached("polygons", make_polygons)

#' The geometry of an sf object as a GeoArrow array
geometry <- function(x) geoarrow::as_geoarrow_array(sf::st_geometry(x))

#' An sf object as the data frame `ga_join()` and `ga_filter()` take
#'
#' The geometry becomes a GeoArrow column and the rest stays as it is. Both
#' functions take it from here in Arrow and hand back an Arrow table, so this
#' conversion is the only R object either of them touches.
as_ga_frame <- function(x) {
  out <- sf::st_drop_geometry(x)
  out$geometry <- geoarrow::as_geoarrow_vctr(geometry(x))
  out
}

#' An sf object as an Arrow table, geometry included as WKB
#'
#' Nothing comes back into R, so the table is what the work runs on. Plain
#' `binary` rather than an extension type, because the predicates read WKB
#' directly and Acero sorts and takes it without knowing it is geometry.
as_arrow <- function(x) {
  out <- arrow::as_arrow_table(sf::st_drop_geometry(x))
  out$geometry <- arrow::as_arrow_array(wk::as_wkb(sf::st_geometry(x)))$cast(
    arrow::binary()
  )
  out
}

elapsed <- function(expr) {
  t0 <- Sys.time()
  force(expr)
  as.numeric(Sys.time() - t0, units = "secs")
}

#' Time one implementation and say what it produced
#'
#' `rows` is reported rather than checked, because the three do not all return
#' the same shape and the differences are the point rather than a fault. With
#' `DUCKSPATIAL_BENCH_LOG` set the same row is appended there, which is how
#' `bench/duckspatial/run-all.R` collects a run.
report <- function(op, n, pkg, seconds, rows, kernel = NA_real_) {
  cat(sprintf(
    "%-11s n = %7s  %-11s %8.2fs%s  %s rows\n",
    op,
    format(n, scientific = FALSE),
    pkg,
    seconds,
    if (is.na(kernel)) "" else sprintf(" (kernel %.2fs)", kernel),
    format(rows, big.mark = ",")
  ))

  log <- Sys.getenv("DUCKSPATIAL_BENCH_LOG", "")
  if (nzchar(log)) {
    started <- file.exists(log)
    write.table(
      data.frame(
        op = op,
        n = as.integer(n),
        pkg = pkg,
        seconds = round(seconds, 3),
        kernel = round(kernel, 3),
        rows = rows
      ),
      log,
      append = started,
      col.names = !started,
      row.names = FALSE,
      sep = ",",
      qmethod = "double"
    )
  }

  invisible(seconds)
}

#' Rows in a duckspatial result, counted after the timer has stopped
#'
#' `ddbs_*()` writes its result to a temp table before handing back a lazy
#' `tbl` over it, so the query has already run when the call returns. Counting
#' scans that table, which is why it is not timed.
ddbs_rows <- function(x) dplyr::pull(dplyr::count(x), n)
