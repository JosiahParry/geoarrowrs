# Run the twelve queries and record the result under bench/last-run/.
#
# Each query gets its own process, because they are timed and a shared one
# carries the last query's memory into the next. Run from the repository root
# against a release build:
#
#   env -u DEBUG Rscript bench/run-all.R
#
# The vignette reads what this writes rather than running anything itself, so
# the article renders on a machine that has neither the data nor the answers.

OUT <- "bench/last-run"
dir.create(OUT, showWarnings = FALSE, recursive = TRUE)

timings <- file.path(OUT, "timings.csv")
unlink(timings)

rscript <- file.path(R.home("bin"), "Rscript")
for (query in sprintf("bench/q%d.R", 1:12)) {
  out <- system2(
    rscript,
    query,
    stdout = TRUE,
    env = paste0("SPATIALBENCH_LOG=", timings)
  )
  cat(out, sep = "\n")
  if (!is.null(attr(out, "status"))) {
    stop(query, " exited with status ", attr(out, "status"), call. = FALSE)
  }
  # a query has been seen to die, exit clean and print nothing, so the row it
  # wrote is what says it ran rather than the status it returned
  name <- tools::file_path_sans_ext(basename(query))
  if (!file.exists(timings) || !name %in% read.csv(timings)$query) {
    stop(query, " wrote no result", call. = FALSE)
  }
}

info <- as.list(Sys.info()[c("sysname", "release", "machine", "nodename")])
write.csv(
  data.frame(
    info,
    cores = parallel::detectCores(),
    r = as.character(getRversion()),
    geoarrowrs = as.character(packageVersion("geoarrowrs")),
    scale_factor = Sys.getenv("SPATIALBENCH_SF", "sf1"),
    run_at = format(Sys.time(), "%Y-%m-%d %H:%M:%S %Z")
  ),
  file.path(OUT, "machine.csv"),
  row.names = FALSE
)
