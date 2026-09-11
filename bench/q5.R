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
