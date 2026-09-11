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
