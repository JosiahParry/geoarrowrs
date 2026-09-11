OUT <- "bench/duckspatial/last-run"
dir.create(OUT, showWarnings = FALSE, recursive = TRUE)

timings <- file.path(OUT, "timings.csv")
unlink(timings)

r <- file.path(R.home("bin"), "R")
ops <- c("connect", "join", "filter", "intersects", "dissolve", "distance")

for (op in ops) {
  out <- system2(
    r,
    c("-q", "-f", file.path("bench/duckspatial", paste0(op, ".R"))),
    stdout = TRUE,
    env = paste0("DUCKSPATIAL_BENCH_LOG=", timings)
  )
  cat(out, sep = "\n")
  if (!is.null(attr(out, "status"))) {
    stop(op, " exited with status ", attr(out, "status"), call. = FALSE)
  }
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
