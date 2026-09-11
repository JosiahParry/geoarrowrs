# Run the five operations and record the result under bench/duckspatial/last-run/.
#
# Each operation gets its own process, because they are timed and a shared one
# carries the last one's memory into the next. Run from the repository root
# against a release build:
#
#   env -u DEBUG Rscript bench/duckspatial/run-all.R

OUT <- "bench/duckspatial/last-run"
dir.create(OUT, showWarnings = FALSE, recursive = TRUE)

timings <- file.path(OUT, "timings.csv")
unlink(timings)

rscript <- file.path(R.home("bin"), "Rscript")
ops <- c("connect", "join", "filter", "intersects", "dissolve", "distance")

for (op in ops) {
  out <- system2(
    rscript,
    file.path("bench/duckspatial", paste0(op, ".R")),
    stdout = TRUE,
    env = paste0("DUCKSPATIAL_BENCH_LOG=", timings)
  )
  cat(out, sep = "\n")
  if (!is.null(attr(out, "status"))) {
    stop(op, " exited with status ", attr(out, "status"), call. = FALSE)
  }
  # an operation has been seen to die, exit clean and print nothing, so the
  # rows it wrote are what say it ran rather than the status it returned
  if (!file.exists(timings) || !op %in% read.csv(timings)$op) {
    stop(op, " wrote no results", call. = FALSE)
  }
}

versions <- function(pkgs) {
  have <- pkgs[vapply(pkgs, requireNamespace, logical(1), quietly = TRUE)]
  stats::setNames(
    lapply(have, function(p) as.character(utils::packageVersion(p))),
    have
  )
}

info <- as.list(Sys.info()[c("sysname", "release", "machine", "nodename")])
write.csv(
  data.frame(
    info,
    cores = parallel::detectCores(),
    r = as.character(getRversion()),
    versions(c("geoarrowrs", "sf", "duckspatial", "duckdb")),
    run_at = format(Sys.time(), "%Y-%m-%d %H:%M:%S %Z")
  ),
  file.path(OUT, "machine.csv"),
  row.names = FALSE
)
