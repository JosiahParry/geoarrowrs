# Q9: building conflation, by intersection over union.
#
# A self join on 20k buildings. The predicate narrows to the pairs whose boxes
# overlap, Acero keeps the ordered half of each pair, and the two sides of the
# intersection are pulled out of the WKB column with `take`, so the geometry
# never leaves Arrow.

source("bench/setup.R")

b <- scan_cols("building", c("b_buildingkey", "b_boundary"))
boundaries <- ga_from_wkb(as_nanoarrow_array(b$b_boundary))
hits <- ga_sparse_intersects(boundaries, boundaries)

# each unordered pair once, the way `b1.id < b2.id` does
ordered <- arrow_table(
  left = left_rows(hits),
  right = right_rows(hits)
) |>
  mutate(
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

run("q9", ordered |>
  mutate(
    building_1 = key_1,
    building_2 = key_2,
    area1 = take(area, left),
    area2 = take(area, right)
  ) |>
  select(building_1, building_2, area1, area2) |>
  compute() |>
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
  collect())
