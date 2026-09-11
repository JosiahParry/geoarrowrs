# Shared setup for the SpatialBench queries.
#
# Queries come from spatialbench-queries/print_queries.py in
# https://github.com/apache/sedona-spatialbench and the answers from
# benchmark/answers/sf<n>/q<k>.csv. Each query is its own file, run on its own
# from the repository root:
#
#   SPATIALBENCH_DATA=~/github/spatialbench-data/v0.1.0 Rscript bench/q1.R
#
# Nothing is brought into R. Relational work runs in Acero, geometry work runs
# in geoarrowrs, and the two meet as Arrow arrays. The one thing Acero cannot
# express is a spatial join, since its joins are equality joins; that match is
# made by a sparse predicate and handed back as a list array, which
# `list_parent_indices` and `list_flatten` turn into row pairs without ever
# materialising them.

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

trip <- dataset("trip")
building <- dataset("building")
customer <- dataset("customer")
zone <- scan_cols("zone", c("z_zonekey", "z_name"))

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

#' One geometry column as a GeoArrow array
#'
#' A spatial predicate needs the whole array at once, which is the one thing
#' Acero cannot hand it.
geometry <- function(name, column, as = ga_from_wkb) {
  col <- scan_cols(name, column)[[column]]
  if (grepl("view", col$type$ToString(), fixed = TRUE)) {
    col <- col$cast(arrow::binary())
  }
  as(as_nanoarrow_array(col))
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

#' Time a query and check it
run <- function(name, expr) {
  t0 <- Sys.time()
  out <- force(expr)
  cat(sprintf("%s: %.2fs\n", name, as.numeric(Sys.time() - t0, units = "secs")))
  check(name, out)
  invisible(out)
}
