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
