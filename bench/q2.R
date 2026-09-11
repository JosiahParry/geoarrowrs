# Q2: count trips starting within Coconino County.
#
# One county against 6M pickups. The predicate puts the county's bounding box
# through an R-tree first, so only the pickups whose box overlaps are relate
# tested, and the answer is the length of the one list it returns.

source("bench/setup.R")

row <- call_function(
  "index",
  zone$z_name,
  options = list(value = Scalar$create("Coconino County"))
)
county <- as_geoarrow_vctr(geometry("zone", "z_boundary"))[as.vector(row) + 1L]

inside <- ga_sparse_intersects(
  county,
  geometry("trip", "t_pickuploc", ga_as_point)
)

run("q2", arrow_table(
  trip_count_in_coconino_county = Array$create(right_rows(inside)$length())
) |>
  collect())
