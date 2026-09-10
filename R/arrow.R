#' Functions that can be registered with Arrow
#'
#' The names [register_geoarrow_udfs()] registers by default, split by how many
#' geometry arguments each one takes.
#'
#' @noRd
geoarrow_udf_catalogue <- function() {
  list(
    unary = c(
      "signed_area",
      "unsigned_area",
      "signed_area_cd",
      "unsigned_area_cd",
      "signed_area_geodesic",
      "unsigned_area_geodesic",
      "perimeter_signed_geodesic",
      "perimeter_unsigned_geodesic",
      "length_euclidean",
      "length_haversine",
      "length_geodesic",
      "length_rhumb",
      "length_vincenty",
      "centroid",
      "interior_point",
      "convex_hull",
      "bounding_rect",
      "minimum_rotated_rect",
      "extremes",
      "is_convex",
      "is_empty",
      "dimension",
      "boundary_dimension",
      "winding_order",
      "is_ccw",
      "is_cw",
      "remove_repeated_points",
      "to_degrees",
      "to_radians",
      "triangulate_earcut",
      "downcast_geometry"
    ),
    binary = c(
      "contains",
      "contains_properly",
      "within",
      "covers",
      "covered_by",
      "intersects",
      "disjoint",
      "touches",
      "crosses",
      "overlaps",
      "equals_topo",
      "relate",
      "coordinate_position",
      "closest_point",
      "closest_point_haversine",
      "line_locate_point",
      "boolean_intersection",
      "boolean_union",
      "boolean_difference",
      "boolean_xor"
    )
  )
}

#' Take a one row sample of a geometry column to infer types from
#' @noRd
geoarrow_udf_sample <- function(x, column) {
  if (inherits(x, "Table") || inherits(x, "RecordBatch")) {
    if (x$num_rows < 1) {
      cli::cli_abort(
        "{.arg x} must have at least one row to infer output types."
      )
    }
    col <- x[[column]]
    if (is.null(col)) {
      cli::cli_abort("Column {.field {column}} not found in {.arg x}.")
    }
    if (inherits(col, "ChunkedArray")) {
      col <- col$chunk(0)
    }
    return(col$Slice(0, 1))
  }

  head_rows <- dplyr::collect(utils::head(x, 1))
  if (nrow(head_rows) < 1) {
    cli::cli_abort("{.arg x} must have at least one row to infer output types.")
  }
  arrow::as_arrow_array(head_rows[[column]])
}

#' Register geoarrowrs functions with Arrow
#'
#' Makes geoarrowrs functions callable inside `dplyr` verbs on an Arrow
#' `Table`, `RecordBatch`, or `Dataset`, so the geometry never has to be pulled
#' into R.
#'
#' @details
#' Arrow's query engine only knows the kernels registered with it, so calling a
#' geoarrowrs function on a `Table` normally warns that the expression is not
#' supported and falls back to pulling the data into R. This registers each
#' function as an Arrow scalar kernel instead, which lets it run inside the
#' engine and, on a `Dataset`, stream batch by batch without ever holding the
#' whole column in memory.
#'
#' A kernel is registered for the exact GeoArrow type of `column` in `x`, since
#' Arrow dispatches on the precise input type. Reading a different geometry
#' type means calling this again with that data. Output types are inferred by
#' running each function once against a single row.
#'
#' Only functions whose arguments are all geometries are registered. Those
#' taking a numeric argument, such as [buffer()] or [simplify()], are not
#' registered, and neither is [unary_union()], which is an aggregate rather
#' than a scalar kernel.
#'
#' @param x an Arrow `Table`, `RecordBatch`, or `Dataset` holding a GeoArrow
#'   column
#' @param column the name of the geometry column to take the type from
#' @param functions names of geoarrowrs functions to register. `NULL`, the
#'   default, registers everything that can be
#' @param prefix prepended to each registered name, to avoid masking an
#'   existing Arrow binding
#' @returns the registered names, invisibly
#' @export
#' @examplesIf requireNamespace("arrow", quietly = TRUE) && requireNamespace("dplyr", quietly = TRUE) && requireNamespace("geoarrow", quietly = TRUE) && requireNamespace("sf", quietly = TRUE)
#' library(dplyr)
#'
#' nc <- as.data.frame(read_shapefile(
#'   system.file("shape/nc.shp", package = "sf")
#' ))
#' tbl <- arrow::arrow_table(nc)
#'
#' register_geoarrow_udfs(tbl)
#'
#' tbl |>
#'   mutate(area = unsigned_area(geometry)) |>
#'   select(NAME, area) |>
#'   head(3) |>
#'   collect()
register_geoarrow_udfs <- function(
  x,
  column = "geometry",
  functions = NULL,
  prefix = ""
) {
  rlang::check_installed(c("arrow", "dplyr"), "to register Arrow kernels.")

  if (!rlang::is_string(column)) {
    cli::cli_abort("{.arg column} must be a single column name.")
  }
  if (!rlang::is_string(prefix)) {
    cli::cli_abort("{.arg prefix} must be a single string.")
  }

  sample <- geoarrow_udf_sample(x, column)
  in_type <- sample$type

  catalogue <- geoarrow_udf_catalogue()
  if (is.null(functions)) {
    functions <- c(catalogue$unary, catalogue$binary)
  }

  unknown <- setdiff(functions, c(catalogue$unary, catalogue$binary))
  if (length(unknown) > 0) {
    cli::cli_abort(c(
      "Cannot register {.fn {unknown}}.",
      i = "Only functions whose arguments are all geometries can be registered."
    ))
  }

  na_sample <- nanoarrow::as_nanoarrow_array(sample)
  registered <- character()
  failed <- character()
  reasons <- character()

  for (nm in functions) {
    fun <- getExportedValue("geoarrowrs", nm)
    arity <- if (nm %in% catalogue$binary) 2L else 1L

    out <- try(
      if (arity == 1L) {
        arrow::as_arrow_array(fun(na_sample))
      } else {
        arrow::as_arrow_array(fun(na_sample, na_sample))
      },
      silent = TRUE
    )
    if (inherits(out, "try-error")) {
      failed <- c(failed, nm)
      reasons <- c(reasons, conditionMessage(attr(out, "condition")))
      next
    }

    kernel <- if (arity == 1L) {
      local({
        f <- fun
        function(context, x) {
          arrow::as_arrow_array(f(nanoarrow::as_nanoarrow_array(x)))
        }
      })
    } else {
      local({
        f <- fun
        function(context, x, y) {
          arrow::as_arrow_array(f(
            nanoarrow::as_nanoarrow_array(x),
            nanoarrow::as_nanoarrow_array(y)
          ))
        }
      })
    }

    schema <- if (arity == 1L) {
      arrow::schema(x = in_type)
    } else {
      arrow::schema(x = in_type, y = in_type)
    }

    ok <- try(
      arrow::register_scalar_function(
        paste0(prefix, nm),
        kernel,
        in_type = schema,
        out_type = out$type,
        auto_convert = FALSE
      ),
      silent = TRUE
    )
    if (inherits(ok, "try-error")) {
      failed <- c(failed, nm)
      reasons <- c(reasons, conditionMessage(attr(ok, "condition")))
    } else {
      registered <- c(registered, paste0(prefix, nm))
    }
  }

  if (length(failed) > 0) {
    cli::cli_warn(c(
      "Could not register {length(failed)} function{?s} for this geometry type.",
      i = "{.fn {failed}}",
      x = "First reason: {reasons[1]}"
    ))
  }

  invisible(registered)
}
