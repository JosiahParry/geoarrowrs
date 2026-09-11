# SpatialBench

The twelve queries of
[SpatialBench](https://github.com/apache/sedona-spatialbench), run by
`bench/run-all.R` and read back here from `bench/last-run/`.

|              |                         |
|:-------------|:------------------------|
| sysname      | Darwin                  |
| release      | 25.4.0                  |
| machine      | arm64                   |
| nodename     | josiah-mpb.local        |
| cores        | 10                      |
| r            | 4.6.1                   |
| geoarrowrs   | 0.0.0.9000              |
| scale_factor | sf1                     |
| run_at       | 2026-09-11 12:35:11 PDT |

| query | seconds | rows | matches |
|:------|--------:|-----:|:--------|
| q1    |    0.63 |   94 | TRUE    |
| q2    |    1.55 |    1 | TRUE    |
| q3    |    0.24 |   22 | TRUE    |
| q4    |    3.42 |  258 | TRUE    |
| q5    |    1.38 |  100 | TRUE    |
| q6    |    4.03 |    3 | TRUE    |
| q7    |    1.42 |  100 | TRUE    |
| q8    |    0.47 |  100 | TRUE    |
| q9    |    0.10 |   37 | TRUE    |
| q10   |    2.56 |  100 | TRUE    |
| q11   |    3.96 |    1 | TRUE    |
| q12   |    3.61 |  100 | TRUE    |

### Shared setup

    bench/setup.R

``` r

# Helpers for the SpatialBench queries.
#
# Queries come from spatialbench-queries/print_queries.py in
# https://github.com/apache/sedona-spatialbench and the answers from
# benchmark/answers/sf<n>/q<k>.csv. Each query is its own file, reads its own
# tables, and times itself, run from the repository root:
#
#   SPATIALBENCH_DATA=~/github/spatialbench-data/v0.1.0 Rscript bench/q1.R
#
# Install a release build first. `devtools::load_all()` and `rextendr::document()`
# set `DEBUG`, which `tools/config.R` turns into a cargo debug build, and geo's
# relate is roughly ten times slower there:
#
#   env -u DEBUG NOT_CRAN=true R CMD INSTALL --no-multiarch .
#
# Nothing is brought into R. Relational work runs in Acero, geometry work runs
# in geoarrowrs, and the two meet as Arrow arrays. The one thing Acero cannot
# express is a spatial join, since its joins are equality joins; that match is
# made by a sparse predicate and handed back as a list array, which
# `list_parent_indices` and `list_flatten` turn into row pairs without ever
# materialising them.
#
# This file defines functions only. It reads no data and starts no clock, so
# what a query costs is what the query file itself does.

library(arrow)
library(dplyr)
library(geoarrow)
library(nanoarrow)
library(geoarrowrs)


SF <- Sys.getenv("SPATIALBENCH_SF", "sf1")
ROOT <- file.path(
  path.expand(Sys.getenv(
    "SPATIALBENCH_DATA",
    "~/github/spatialbench-data/v0.1.0"
  )),
  SF
)

dataset <- function(name) open_dataset(file.path(ROOT, name))

#' Columns of a table, as an Arrow Table
#'
#' Scanner rather than `select()`, because `zone` stores its strings and
#' geometry as `string_view` and `binary_view`, which the dplyr path rejects.
#' The scanner normalises them back.
scan_cols <- function(name, columns) {
  Scanner$create(dataset(name), projection = columns)$ToTable()
}

#' A stored timestamp, ready to render as the value it holds
#'
#' The stored timestamps are naive and the answers read them as UTC. Acero's
#' `strftime()` renders in the session's own zone unless it is told otherwise,
#' so every call on one of these has to pass `tz = "UTC"` as well, or the
#' result silently follows whatever zone the machine is set to. Seconds rather
#' than milliseconds, because `strftime()` gives a millisecond timestamp a
#' `.000` the answers do not have.
utc_time <- function(column) {
  cast(column, timestamp(unit = "s", timezone = "UTC"))
}

# the literal shapes two of the queries measure against
BOX_Q3_WKT <- paste0(
  "POLYGON ((-111.9060 34.7347, -111.6160 34.7347, -111.6160 35.0047, ",
  "-111.9060 35.0047, -111.9060 34.7347))"
)
BOX_Q6_WKT <- paste0(
  "POLYGON ((-112.2110 34.4197, -111.3110 34.4197, -111.3110 35.3197, ",
  "-112.2110 35.3197, -112.2110 34.4197))"
)

#' A literal geometry, as the length 1 array a kernel recycles
literal <- function(wkt) as_geoarrow_array(wk::wkt(wkt))

#' One geometry column as an Arrow WKB array
#'
#' The `zone` table stores its geometry as `binary_view`, which has to be cast
#' before anything will read it.
wkb <- function(name, column) {
  col <- scan_cols(name, column)[[column]]
  if (grepl("view", col$type$ToString(), fixed = TRUE)) {
    col <- col$cast(arrow::binary())
  }
  col
}

#' One geometry column, ready to hand to geoarrowrs
#'
#' A spatial predicate needs the whole array at once, which is the one thing
#' Acero cannot hand it. The WKB goes straight in rather than through
#' `ga_from_wkb()`: every function here reads WKB, and converting to a native
#' encoding first parses every coordinate twice, once into the native array and
#' again into the geometries the predicate walks.
geometry <- function(name, column) {
  as_nanoarrow_array(wkb(name, column))
}

#' Rows of an Arrow column, by the 1 based index a predicate returns
take <- function(column, index) {
  call_function(
    "take",
    column,
    call_function("subtract", index$cast(int64()), Scalar$create(1L, int64()))
  )
}

#' The row of `x` each match came from, 1 based
left_rows <- function(hits) {
  call_function(
    "add",
    call_function("list_parent_indices", as_arrow_array(hits)),
    Scalar$create(1L, int64())
  )
}

#' The row of `y` each match landed on, 1 based
right_rows <- function(hits) {
  call_function("list_flatten", as_arrow_array(hits))
}

#' The `row` and `distance` pairs a nearest neighbour search returns
knn_pairs <- function(hits) {
  call_function("list_flatten", as_arrow_array(hits))
}

#' Attach a computed Arrow column to a table
with_column <- function(table, name, values) {
  table[[name]] <- as_arrow_array(values)
  table
}

#' Compare a result against the committed answer, on values rather than order
check <- function(name, got) {
  answers <- Sys.getenv("SPATIALBENCH_ANSWERS", "")
  path <- file.path(answers, SF, paste0(name, ".csv"))
  if (!nzchar(answers) || !file.exists(path)) {
    cat(name, ":", nrow(got), "rows (no answer to check against)\n")
    return(invisible(NA))
  }

  want <- read.csv(path, stringsAsFactors = FALSE)
  if (!setequal(names(want), names(got))) {
    cat(
      name,
      ": columns differ\n  got  ",
      paste(names(got), collapse = ", "),
      "\n  want ",
      paste(names(want), collapse = ", "),
      "\n"
    )
    return(invisible(FALSE))
  }
  if (nrow(want) != nrow(got)) {
    cat(name, ":", nrow(got), "rows, expected", nrow(want), "\n")
    return(invisible(FALSE))
  }

  # the committed answers are written with a few significant figures, so a
  # tighter tolerance than that would fail on the file rather than on the query
  got <- as.data.frame(got)[names(want)]
  bad <- names(want)[
    !vapply(
      names(want),
      function(nm) {
        if (is.numeric(want[[nm]])) {
          isTRUE(all.equal(want[[nm]], as.numeric(got[[nm]]), tolerance = 1e-4))
        } else {
          identical(as.character(want[[nm]]), as.character(got[[nm]]))
        }
      },
      logical(1)
    )
  ]

  if (length(bad)) {
    cat(name, ": differs in", paste(bad, collapse = ", "), "\n")
    for (nm in bad) {
      i <- which(as.character(want[[nm]]) != as.character(got[[nm]]))[1]
      cat(sprintf(
        "  %s row %d: got %s, want %s\n",
        nm,
        i,
        format(got[[nm]][i]),
        format(want[[nm]][i])
      ))
    }
    invisible(FALSE)
  } else {
    cat(name, ": matches ground truth,", nrow(got), "rows\n")
    invisible(TRUE)
  }
}

#' Report how long a query took and whether it is right
#'
#' `t0` is taken in the query file, so the time covers the reads, the geometry
#' and the aggregation rather than whichever step happens to come last. With
#' `SPATIALBENCH_LOG` set the same row is appended there, which is how
#' `bench/run-all.R` collects a run without parsing this output back.
report <- function(name, t0, got) {
  seconds <- as.numeric(Sys.time() - t0, units = "secs")
  cat(sprintf("%s: %.2fs\n", name, seconds))
  matches <- check(name, got)

  log <- Sys.getenv("SPATIALBENCH_LOG", "")
  if (nzchar(log)) {
    started <- file.exists(log)
    row <- data.frame(
      query = name,
      seconds = round(seconds, 2),
      rows = nrow(got),
      matches = matches
    )
    write.table(
      row,
      log,
      append = started,
      col.names = !started,
      row.names = FALSE,
      sep = ",",
      qmethod = "double"
    )
  }

  invisible(got)
}
```

### Query 1

    bench/q1.R

``` r

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
```

### Query 2

    bench/q2.R

``` r

# Q2: count trips starting within Coconino County.
#
# One county against 6M pickups. Only the one county's WKB is parsed: the row is
# found on the name column and `take`n out of the boundary column first, so the
# other 156k polygons are never turned into geometries. The predicate then puts
# that county's bounding box through an R-tree, so only the pickups whose box
# overlaps are tested.

source("bench/setup.R")

t0 <- Sys.time()

z <- scan_cols("zone", c("z_name", "z_boundary"))
row <- as.vector(call_function(
  "index",
  z$z_name,
  options = list(value = Scalar$create("Coconino County"))
))
county_wkb <- z$z_boundary$Slice(row, 1)$cast(arrow::binary())
county <- as_nanoarrow_array(county_wkb)

inside <- ga_sparse_intersects(
  county,
  geometry("trip", "t_pickuploc")
)

out <- arrow_table(
  trip_count_in_coconino_county = Array$create(right_rows(inside)$length())
) |>
  collect()

report("q2", t0, out)
```

### Query 3

    bench/q3.R

``` r

# Q3: monthly trip statistics for a buffered box around Sedona city center.
#
# Acero takes no geometry constants, so the distance to the literal box is
# measured by the kernel and attached to the table as an ordinary column. The
# filter, the monthly rollup and the averages then all run in the engine.

source("bench/setup.R")

t0 <- Sys.time()

t <- scan_cols("trip", c(
  "t_tripkey", "t_pickuptime", "t_dropofftime", "t_distance", "t_fare"
))
t <- with_column(t, "distance_to_box", ga_dist_euclidean_pairwise(
  geometry("trip", "t_pickuploc"),
  literal(BOX_Q3_WKT)
))

out <- t |>
  filter(distance_to_box <= 0.045) |>
  mutate(
    pickup_month = strftime(
      utc_time(t_pickuptime),
      format = "%Y-%m-01",
      tz = "UTC"
    ),
    duration = (cast(t_dropofftime, int64()) - cast(t_pickuptime, int64())) /
      1000
  ) |>
  group_by(pickup_month) |>
  summarise(
    total_trips = n(),
    avg_distance = mean(cast(t_distance, float64())),
    avg_duration_seconds = mean(duration),
    avg_fare = mean(cast(t_fare, float64()))
  ) |>
  arrange(pickup_month) |>
  collect()

report("q3", t0, out)
```

### Query 4

    bench/q4.R

``` r

# Q4: zone distribution of the top 1000 trips by tip.
#
# The ranking is a plain Acero sort, so only 1000 rows leave the engine. Those
# pickups are matched to zones by a sparse predicate, and the zone attributes
# are pulled out by index with `take` rather than joined, so the counting is a
# straight group_by on an Arrow table.

source("bench/setup.R")

t0 <- Sys.time()

top <- dataset("trip") |>
  arrange(desc(t_tip), t_tripkey) |>
  head(1000) |>
  select(t_pickuploc) |>
  compute()

zone <- scan_cols("zone", c("z_zonekey", "z_name"))
hits <- ga_sparse_within(
  as_nanoarrow_array(top$t_pickuploc),
  geometry("zone", "z_boundary")
)
matched <- right_rows(hits)

out <- arrow_table(
  z_zonekey = take(zone$z_zonekey, matched),
  z_name = take(zone$z_name, matched)
) |>
  count(z_zonekey, z_name, name = "trip_count") |>
  arrange(desc(trip_count), z_zonekey) |>
  collect()

report("q4", t0, out)
```

### Query 5

    bench/q5.R

``` r

# Q5: monthly travel hull area for repeat customers.
#
# The one query needing a grouped aggregate, which Acero cannot express: it
# registers scalar kernels only, so there is no way to hand it a per group
# convex hull. Acero still does all the relational work. It joins the trips to
# their customers, buckets them by month, and orders the rows by the grouping;
# the same ordering gives the size of each group, and `ga_collect_agg()` takes
# those run lengths and returns one collection per group. The hull and its area
# are then ordinary vectorized calls over the 316k groups.

source("bench/setup.R")

t0 <- Sys.time()

customer <- scan_cols("customer", c("c_custkey", "c_name"))
trip <- scan_cols("trip", c("t_custkey", "t_pickuptime", "t_dropoffloc"))

# the month each trip falls in, carrying its dropoff along rather than joining
# it back on by position, which no step here promises to preserve
by_month <- trip |>
  mutate(
    pickup_month = strftime(
      utc_time(t_pickuptime),
      format = "%Y-%m-01",
      tz = "UTC"
    )
  ) |>
  select(t_custkey, pickup_month, t_dropoffloc) |>
  compute()

# only the customer-months with more than five dropoffs, ordered by the grouping
groups <- by_month |>
  count(t_custkey, pickup_month, name = "dropoff_count") |>
  filter(dropoff_count > 5) |>
  arrange(t_custkey, pickup_month) |>
  compute()

# the dropoffs of those groups, in that same order
rows <- by_month |>
  inner_join(
    select(groups, t_custkey, pickup_month),
    by = c("t_custkey", "pickup_month")
  ) |>
  arrange(t_custkey, pickup_month) |>
  compute()

hulls <- ga_convex_hull(ga_collect_agg(
  as_nanoarrow_array(rows$t_dropoffloc),
  sizes = as.vector(groups$dropoff_count)
))

out <- arrow_table(
  c_custkey = groups$t_custkey,
  pickup_month = groups$pickup_month,
  dropoff_count = groups$dropoff_count
) |>
  with_column("monthly_travel_hull_area", ga_unsigned_area(hulls)) |>
  left_join(customer, by = c("c_custkey" = "c_custkey")) |>
  mutate(customer_name = c_name) |>
  select(
    c_custkey,
    customer_name,
    pickup_month,
    monthly_travel_hull_area,
    dropoff_count
  ) |>
  arrange(desc(monthly_travel_hull_area), c_custkey, pickup_month) |>
  head(100) |>
  collect()

report("q5", t0, out)
```

### Query 6

    bench/q6.R

``` r

# Q6: zone statistics for trips inside zones that meet a bounding box.
#
# Two spatial steps, both sparse predicates: the zones the literal box meets,
# then the pickups those zones contain. Zone attributes come out by index with
# `take`, the trip measures by index too, and the rollup runs in Acero.

source("bench/setup.R")

t0 <- Sys.time()

zone <- scan_cols("zone", c("z_zonekey", "z_name"))
boundaries <- wkb("zone", "z_boundary")
in_box <- right_rows(ga_sparse_intersects(
  literal(BOX_Q6_WKT),
  as_nanoarrow_array(boundaries)
))

# the handful of zones the box met, taken out of the WKB rather than converted
wanted <- as_nanoarrow_array(take(boundaries, in_box))
hits <- ga_sparse_contains(
  wanted,
  geometry("trip", "t_pickuploc")
)

t <- scan_cols("trip", c("t_pickuptime", "t_dropofftime", "t_distance"))
zone_row <- take(in_box, left_rows(hits))
trip_row <- right_rows(hits)

out <- arrow_table(
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
  collect()

report("q6", t0, out)
```

### Query 7

    bench/q7.R

``` r

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
```

### Query 8

    bench/q8.R

``` r

# Q8: pickups within 500m of each building.
#
# `ga_sparse_dwithin()` is ST_DWithin: it grows each building's box by the
# radius, then measures the candidates exactly. Buffering and intersecting
# instead would undercount, because a buffer approximates its arcs with
# segments. Counting the matches per building is a group_by in Acero.
#
# The query asks for 500m and this passes 0.0045 degrees, which is 500m of
# latitude and about 414m of longitude at this latitude. That is wrong, and it
# is what the committed answer measures: Sedona's ST_DWithin is planar on the
# raw coordinates, so `metric = "euclidean"` is what reproduces it. The right
# answer for longitude and latitude is `metric = "geodesic"` with 500, which
# this deliberately does not use, because the point here is to match.

source("bench/setup.R")

t0 <- Sys.time()

b <- scan_cols("building", c("b_buildingkey", "b_name"))
near <- ga_sparse_dwithin(
  geometry("building", "b_boundary"),
  geometry("trip", "t_pickuploc"),
  0.0045,
  metric = "euclidean"
)

building_row <- left_rows(near)

out <- arrow_table(
  b_buildingkey = take(b$b_buildingkey, building_row),
  b_name = take(b$b_name, building_row)
) |>
  count(b_buildingkey, b_name, name = "nearby_pickup_count") |>
  arrange(desc(nearby_pickup_count), b_buildingkey) |>
  head(100) |>
  collect()

report("q8", t0, out)
```

### Query 9

    bench/q9.R

``` r

# Q9: building conflation, by intersection over union.
#
# A self join on 20k buildings. The predicate narrows to the pairs whose boxes
# overlap, Acero keeps the ordered half of each pair, and the two sides of the
# intersection are pulled out of the WKB column with `take`, so the geometry
# never leaves Arrow.

source("bench/setup.R")

t0 <- Sys.time()

b <- scan_cols("building", c("b_buildingkey", "b_boundary"))
boundaries <- ga_from_wkb(as_nanoarrow_array(b$b_boundary))
hits <- ga_sparse_intersects(boundaries, boundaries)

# each unordered pair once, the way `b1.id < b2.id` does. The keys are taken
# before the filter, since `take` is not an expression Acero can plan.
left <- left_rows(hits)
right <- right_rows(hits)

ordered <- arrow_table(
  left = left,
  right = right,
  key_1 = take(b$b_buildingkey, left),
  key_2 = take(b$b_buildingkey, right)
) |>
  filter(key_1 < key_2) |>
  compute()

overlap <- ga_boolean_intersection(
  ga_from_wkb(as_nanoarrow_array(take(b$b_boundary, ordered$left))),
  ga_from_wkb(as_nanoarrow_array(take(b$b_boundary, ordered$right)))
)
area <- as_arrow_array(ga_unsigned_area(boundaries))

out <- arrow_table(
  building_1 = ordered$key_1,
  building_2 = ordered$key_2,
  area1 = take(area, ordered$left),
  area2 = take(area, ordered$right)
) |>
  with_column("overlap_area", ga_unsigned_area(overlap)) |>
  mutate(
    iou = if_else(
      overlap_area == 0,
      0,
      overlap_area / (area1 + area2 - overlap_area)
    )
  ) |>
  arrange(desc(iou), building_1, building_2) |>
  head(100) |>
  collect()

report("q9", t0, out)
```

### Query 10

    bench/q10.R

``` r

# Q10: zone statistics for the trips starting inside each zone.
#
# A left join: every zone appears, including those no trip starts in. The
# spatial match gives the pairs, Acero aggregates them, and a left_join back
# onto the zone table restores the zones with no trips.

source("bench/setup.R")

t0 <- Sys.time()

zone <- scan_cols("zone", c("z_zonekey", "z_name"))
hits <- ga_sparse_contains(
  geometry("zone", "z_boundary"),
  geometry("trip", "t_pickuploc")
)

t <- scan_cols("trip", c("t_pickuptime", "t_dropofftime", "t_distance"))
zone_row <- left_rows(hits)
trip_row <- right_rows(hits)

stats <- arrow_table(
  z_zonekey = take(zone$z_zonekey, zone_row),
  distance = take(t$t_distance, trip_row),
  pickup = take(t$t_pickuptime, trip_row),
  dropoff = take(t$t_dropofftime, trip_row)
) |>
  mutate(
    duration = (cast(dropoff, int64()) - cast(pickup, int64())) / 1000
  ) |>
  group_by(z_zonekey) |>
  summarise(
    avg_duration_seconds = mean(duration),
    avg_distance = mean(cast(distance, float64())),
    num_trips = n()
  ) |>
  compute()

out <- zone |>
  left_join(stats, by = "z_zonekey") |>
  mutate(
    pickup_zone = z_name,
    num_trips = coalesce(num_trips, 0L)
  ) |>
  select(
    z_zonekey,
    pickup_zone,
    avg_duration_seconds,
    avg_distance,
    num_trips
  ) |>
  arrange(is.na(avg_duration_seconds), desc(avg_duration_seconds), z_zonekey) |>
  head(100) |>
  collect()

report("q10", t0, out)
```

### Query 11

    bench/q11.R

``` r

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
```

### Query 12

    bench/q12.R

``` r

# Q12: pickups ranked by mean distance to their 5 nearest buildings.
#
# A nearest neighbour join. `ga_sparse_knn()` answers all 6M queries in one
# call and returns five rows each with the distance already measured, so there
# is no second pass over the geometry; the per trip mean runs in Acero.
#
# `metric = "euclidean"` ranks and measures in degrees, which is what the
# committed answer holds, because Sedona measures planar. It is also the only
# metric available here: the buildings are polygons, and geo defines the
# spherical and ellipsoidal metrics between points alone, so there is no
# geodesic distance from a pickup to a building's nearest edge to ask for.

source("bench/setup.R")

t0 <- Sys.time()

b <- scan_cols("building", "b_boundary")
t <- scan_cols("trip", c("t_tripkey", "t_pickuploc"))

nearest <- ga_sparse_knn(
  as_nanoarrow_array(t$t_pickuploc),
  as_nanoarrow_array(b$b_boundary),
  k = 5,
  metric = "euclidean"
)

out <- arrow_table(
  t_tripkey = take(t$t_tripkey, left_rows(nearest)),
  distance = knn_pairs(nearest)$GetFieldByName("distance")
) |>
  group_by(t_tripkey) |>
  summarise(avg_distance_to_5_nearest = mean(distance)) |>
  arrange(desc(avg_distance_to_5_nearest), t_tripkey) |>
  head(100) |>
  collect()

report("q12", t0, out)
```
