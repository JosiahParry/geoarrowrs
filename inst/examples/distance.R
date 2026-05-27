library(sf)
library(wk)
library(geoarrow)
devtools::load_all()

set.seed(0)

n <- 100
o <- xy(runif(n, -180, 180), runif(n, -90, 90))
d <- xy(runif(n, -180, 180), runif(n, -90, 90))

bench::mark(
  geoarrow_rs = as.vector(distance_euclidean(
    as_geoarrow_array(o),
    as_geoarrow_array(d)
  )),
  sf = st_distance(st_as_sfc(o), st_as_sfc(d), by_element = TRUE)
)
