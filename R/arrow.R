#' The GeoArrow geometry types kernels are registered for
#'
#' Arrow dispatches on the exact input type, so a kernel is registered for
#' every one of these rather than for whichever type happens to be in a table.
#'
#' @noRd
geoarrow_udf_geometry_types <- function() {
  c(
    "POINT",
    "LINESTRING",
    "POLYGON",
    "MULTIPOINT",
    "MULTILINESTRING",
    "MULTIPOLYGON"
  )
}

#' Argument specifications for every registerable function
#'
#' Each entry is a named vector of argument kinds, in order. `geometry` is a
#' GeoArrow column, `array` a numeric Arrow array that is recycled row by row,
#' and `num`, `int`, `bool`, and `string` are scalar options. A `=` suffix
#' gives the value used when probing for the output type.
#'
#' @noRd
geoarrow_udf_catalogue <- function() {
  unary <- c(
    "ga_signed_area",
    "ga_unsigned_area",
    "ga_signed_area_cd",
    "ga_unsigned_area_cd",
    "ga_signed_area_geodesic",
    "ga_unsigned_area_geodesic",
    "ga_perimeter_signed_geodesic",
    "ga_perimeter_unsigned_geodesic",
    "ga_length_euclidean",
    "ga_length_haversine",
    "ga_length_geodesic",
    "ga_length_rhumb",
    "ga_length_vincenty",
    "ga_centroid",
    "ga_interior_point",
    "ga_convex_hull",
    "ga_bounding_rect",
    "ga_minimum_rotated_rect",
    "ga_extremes",
    "ga_is_convex",
    "ga_is_empty",
    "ga_is_valid",
    "ga_validation_error",
    "ga_dimension",
    "ga_boundary_dimension",
    "ga_winding_order",
    "ga_is_ccw",
    "ga_is_cw",
    "ga_remove_repeated_points",
    "ga_to_degrees",
    "ga_to_radians",
    "ga_triangulate_earcut",
    "ga_downcast_geometry",
    "ga_coords",
    "ga_exterior_coords",
    "ga_n_coords",
    "ga_lines",
    "ga_self_intersections"
  )

  binary <- c(
    "ga_contains",
    "ga_contains_properly",
    "ga_within",
    "ga_covers",
    "ga_covered_by",
    "ga_intersects",
    "ga_disjoint",
    "ga_touches",
    "ga_crosses",
    "ga_overlaps",
    "ga_equals_topo",
    "ga_relate",
    "ga_coordinate_position",
    "ga_closest_point",
    "ga_closest_point_haversine",
    "ga_line_locate_point",
    "ga_line_intersection",
    "ga_boolean_intersection",
    "ga_boolean_union",
    "ga_boolean_difference",
    "ga_boolean_xor",
    "ga_bearing_euclidean",
    "ga_bearing_geodesic",
    "ga_bearing_haversine",
    "ga_bearing_rhumb",
    "ga_dist_euclidean_pairwise",
    "ga_dist_frechet_pairwise",
    "ga_dist_geodesic_pairwise",
    "ga_dist_hausdorff_pairwise",
    "ga_dist_haversine_pairwise",
    "ga_dist_rhumb_pairwise",
    "ga_dist_vincenty_pairwise"
  )

  catalogue <- c(
    stats::setNames(rep(list(c(x = "geometry")), length(unary)), unary),
    stats::setNames(
      rep(list(c(x = "geometry", y = "geometry")), length(binary)),
      binary
    )
  )

  c(
    catalogue,
    list(
      ga_simplify = c(geometry = "geometry", epsilon = "array"),
      ga_simplify_vw = c(geometry = "geometry", epsilon = "array"),
      ga_simplify_vw_preserve = c(geometry = "geometry", epsilon = "array"),
      ga_simplify_idx = c(geometry = "geometry", epsilon = "array"),
      ga_simplify_vw_idx = c(geometry = "geometry", epsilon = "array"),
      ga_rotate_around_center = c(geometry = "geometry", degrees = "array"),
      ga_rotate_around_centroid = c(geometry = "geometry", degrees = "array"),
      ga_skew = c(geometry = "geometry", degrees = "array"),
      ga_translate = c(
        geometry = "geometry",
        x_offset = "array",
        y_offset = "array"
      ),
      ga_scale_xy = c(
        geometry = "geometry",
        x_factor = "array",
        y_factor = "array"
      ),
      ga_skew_xy = c(
        geometry = "geometry",
        degrees_x = "array",
        degrees_y = "array"
      ),
      ga_affine_transform = c(
        geometry = "geometry",
        a = "array",
        b = "array",
        xoff = "array",
        d = "array",
        e = "array",
        yoff = "array"
      ),
      ga_concave_hull = c(
        geometry = "geometry",
        concavity = "array",
        length_threshold = "array"
      ),
      ga_dbscan = c(geometry = "geometry", eps = "array", min_points = "array"),
      ga_kmeans = c(geometry = "geometry", k = "array"),
      ga_outlier_scores = c(geometry = "geometry", k_neighbours = "array"),
      ga_chaikin_smoothing = c(geometry = "geometry", n_iterations = "int=1"),
      ga_line_segmentize = c(geometry = "geometry", segment_count = "int=2"),
      ga_line_segmentize_haversine = c(
        geometry = "geometry",
        segment_count = "int=2"
      ),
      ga_densify = c(
        geometry = "geometry",
        max_segment_length = "array",
        metric = "string=euclidean"
      ),
      ga_buffer = c(
        geometry = "geometry",
        distance = "array",
        line_cap = "string=round",
        line_join = "string=round",
        miter_limit = "num=2",
        round_segments = "num=8"
      ),
      ga_triangulate_delaunay = c(
        x = "geometry",
        constrained = "bool=TRUE",
        snap_radius = "array"
      ),
      ga_orient = c(geometry = "geometry", direction = "string=default"),
      ga_interpolate_point = c(
        line = "geometry",
        value = "array",
        metric = "string=euclidean",
        measure = "string=ratio",
        from = "string=start"
      ),
      ga_dest_euclidean = c(
        origin = "geometry",
        bearing = "array",
        distance = "array"
      ),
      ga_dest_haversine = c(
        origin = "geometry",
        bearing = "array",
        distance = "array"
      ),
      ga_dest_geodesic = c(
        origin = "geometry",
        bearing = "array",
        distance = "array"
      ),
      ga_dest_rhumb = c(
        origin = "geometry",
        bearing = "array",
        distance = "array"
      ),
      ga_point_at_distance_between = c(
        start = "geometry",
        end = "geometry",
        distance = "array",
        metric = "string=euclidean"
      ),
      ga_point_at_ratio_between = c(
        start = "geometry",
        end = "geometry",
        ratio = "array",
        metric = "string=euclidean"
      ),
      ga_points_along_line = c(
        start = "geometry",
        end = "geometry",
        max_distance = "array",
        include_ends = "bool=TRUE",
        metric = "string=euclidean"
      )
    )
  )
}

#' Split an argument specification into its kind and its probe value
#' @noRd
geoarrow_udf_kind <- function(spec) {
  sub("=.*$", "", spec)
}

#' @rdname geoarrow_udf_kind
#' @noRd
geoarrow_udf_probe <- function(spec) {
  ifelse(
    grepl("=", spec, fixed = TRUE),
    sub("^[^=]*=", "", spec),
    NA_character_
  )
}

#' Build the GeoArrow schemas kernels dispatch on
#'
#' One schema per geometry type, all carrying the same extension metadata so
#' that a kernel matches a column with that CRS.
#'
#' @noRd
geoarrow_udf_schemas <- function(extension_metadata) {
  schemas <- lapply(geoarrow_udf_geometry_types(), function(type) {
    schema <- geoarrow::na_extension_geoarrow(type)
    if (!is.null(extension_metadata)) {
      schema$metadata[["ARROW:extension:metadata"]] <- extension_metadata
    }
    list(
      type = arrow::as_data_type(schema),
      empty = geoarrow_udf_empty(schema)
    )
  })
  stats::setNames(schemas, geoarrow_udf_geometry_types())
}

#' Read the GeoArrow extension metadata for every CRS the caller asked for
#' @noRd
geoarrow_udf_metadatas <- function(crs) {
  crs <- if (is.null(crs)) {
    list(NULL)
  } else if (is.character(crs)) {
    as.list(crs)
  } else if (rlang::is_bare_list(crs)) {
    crs
  } else {
    list(crs)
  }
  unique(lapply(crs, geoarrow_udf_metadata))
}

#' Read the GeoArrow extension metadata out of whatever the caller passed
#' @noRd
geoarrow_udf_metadata <- function(crs) {
  if (is.null(crs)) {
    return(NULL)
  }

  if (rlang::is_string(crs)) {
    schema <- geoarrow::na_extension_geoarrow("POINT", crs = crs)
    return(schema$metadata[["ARROW:extension:metadata"]])
  }

  if (inherits(crs, c("Table", "RecordBatch", "Dataset", "Schema"))) {
    fields <- if (inherits(crs, "Schema")) crs else crs$schema
    types <- lapply(names(fields), function(nm) fields[[nm]]$type)
    is_geo <- vapply(types, inherits, logical(1), "ExtensionType")
    if (!any(is_geo)) {
      cli::cli_abort("{.arg crs} has no GeoArrow column to take a CRS from.")
    }
    crs <- types[[which(is_geo)[1]]]
  } else if (inherits(crs, c("Array", "ChunkedArray"))) {
    crs <- crs$type
  }

  schema <- rlang::try_fetch(
    nanoarrow::as_nanoarrow_schema(crs),
    error = function(cnd) nanoarrow::infer_nanoarrow_schema(crs)
  )
  schema$metadata[["ARROW:extension:metadata"]]
}

#' A zero length array to probe a function's output type with
#' @noRd
geoarrow_udf_empty <- function(schema) {
  array <- geoarrow::as_geoarrow_array(character(0), schema = schema)
  nanoarrow::nanoarrow_array_set_schema(array, schema, validate = FALSE)
  array
}

#' The Arrow type a kernel argument dispatches on
#' @noRd
geoarrow_udf_type <- function(kind, geometry_type) {
  switch(
    kind,
    geometry = geometry_type,
    array = arrow::float64(),
    num = arrow::float64(),
    int = arrow::int32(),
    bool = arrow::boolean(),
    string = arrow::utf8()
  )
}

#' A stand in value used to probe a function's output type
#' @noRd
geoarrow_udf_value <- function(kind, probe, geometry_schema) {
  switch(
    kind,
    geometry = if (!is.null(geometry_schema)) {
      geoarrow_udf_empty(geometry_schema)
    },
    array = nanoarrow::as_nanoarrow_array(double()),
    num = if (is.na(probe)) 1 else as.numeric(probe),
    int = if (is.na(probe)) 1L else as.integer(probe),
    bool = if (is.na(probe)) TRUE else as.logical(probe),
    string = probe
  )
}

#' The Arrow types a bare WKB column arrives as
#'
#' Plain Parquet stores geometry as binary with no GeoArrow extension name on
#' it, so kernels are registered for those two storage types as well.
#'
#' @noRd
geoarrow_udf_wkb_types <- function() {
  list(binary = arrow::binary(), large_binary = arrow::large_binary())
}

#' Is this nanoarrow array a bare WKB column rather than a GeoArrow one
#' @noRd
geoarrow_udf_is_wkb <- function(array) {
  nanoarrow::infer_nanoarrow_schema(array)$format %in% c("z", "Z")
}

#' Give a GeoArrow result the metadata encoding Arrow can carry
#'
#' geoarrow-rs writes no `ARROW:extension:metadata` key when an array has no
#' CRS. Arrow hands that back as an empty string, which is not JSON, so the
#' next kernel to read the column fails while parsing it. Writing the empty
#' object geoarrow R writes keeps a column readable across kernels.
#'
#' @noRd
geoarrow_udf_normalise <- function(array) {
  if (!geoarrow_udf_is_geoarrow(array)) {
    return(array)
  }
  schema <- nanoarrow::infer_nanoarrow_schema(array)
  if (is.null(schema$metadata[["ARROW:extension:metadata"]])) {
    schema$metadata[["ARROW:extension:metadata"]] <- "{}"
    nanoarrow::nanoarrow_array_set_schema(array, schema, validate = FALSE)
  }
  array
}

#' Does this nanoarrow array carry a GeoArrow extension name
#' @noRd
geoarrow_udf_is_geoarrow <- function(array) {
  name <- nanoarrow::infer_nanoarrow_schema(array)$metadata[[
    "ARROW:extension:name"
  ]]
  !is.null(name) && startsWith(name, "geoarrow.")
}

#' Turn the arguments Arrow hands a kernel into what the function expects
#' @noRd
geoarrow_udf_coerce <- function(args, kinds, mode) {
  Map(
    function(arg, kind) {
      if (!kind %in% c("geometry", "array")) {
        return(as.vector(arg))
      }
      array <- nanoarrow::as_nanoarrow_array(arg)
      if (kind == "geometry" && geoarrow_udf_is_wkb(array)) {
        array <- switch(
          mode,
          pass = array,
          downcast = ga_from_wkb(array),
          ga_cast_geometry(array, mode)
        )
      }
      array
    },
    args,
    kinds
  )
}

#' Wrap a geoarrowrs function so Arrow can call it
#'
#' A geometry argument that arrives as bare WKB is parsed on the way in, and a
#' geometry result is written back out as WKB, so the kernel's declared output
#' type holds whatever the column turns out to contain.
#'
#' @noRd
geoarrow_udf_kernel <- function(fun, kinds, mode) {
  force(fun)
  force(kinds)
  mode <- mode %||% "downcast"
  function(context, ...) {
    args <- list(...)
    wkb <- any(vapply(
      which(kinds == "geometry"),
      function(i) geoarrow_udf_is_wkb(nanoarrow::as_nanoarrow_array(args[[i]])),
      logical(1)
    ))

    result <- rlang::inject(fun(!!!geoarrow_udf_coerce(args, kinds, mode)))
    if (geoarrow_udf_is_geoarrow(result)) {
      if (wkb) {
        result <- ga_cast_geometry(result, "wkb")
      }
      result <- geoarrow_udf_normalise(result)
    }
    arrow::as_arrow_array(result)
  }
}

#' Work out the kernels one function can offer for a bare WKB column
#'
#' The output type cannot depend on what the WKB turns out to hold, so a
#' geometry result is declared as WKB too. Everything else keeps the type it
#' has whatever the input geometry type was.
#'
#' @noRd
geoarrow_udf_wkb_kernels <- function(fun, spec) {
  kinds <- geoarrow_udf_kind(spec)
  probes <- geoarrow_udf_probe(spec)
  slots <- which(kinds == "geometry")
  prototypes <- geoarrow_udf_schemas(NULL)

  out <- NULL
  accepts <- character()
  for (type in names(prototypes)) {
    values <- Map(geoarrow_udf_value, kinds, probes, MoreArgs = list(NULL))
    for (slot in slots) {
      values[[slot]] <- prototypes[[type]]$empty
    }
    candidate <- rlang::try_fetch(
      rlang::inject(fun(!!!unname(values))),
      error = function(cnd) NULL
    )
    if (!is.null(candidate)) {
      accepts <- c(accepts, type)
      out <- out %||% candidate
    }
  }

  if (is.null(out)) {
    return(list(in_types = list(), out_types = list(), mode = NULL))
  }

  out_type <- if (geoarrow_udf_is_geoarrow(out)) {
    arrow::as_data_type(geoarrow::na_extension_wkb())
  } else {
    arrow::as_arrow_array(out)$type
  }

  wkb <- geoarrow_udf_wkb_types()
  grid <- expand.grid(
    rep(list(names(wkb)), length(slots)),
    stringsAsFactors = FALSE
  )

  in_types <- list()
  for (row in seq_len(nrow(grid))) {
    types <- as.character(grid[row, ])
    fields <- lapply(kinds, geoarrow_udf_type, geometry_type = NULL)
    names(fields) <- names(spec)
    for (i in seq_along(slots)) {
      fields[[slots[i]]] <- wkb[[types[i]]]
    }
    in_types[[length(in_types) + 1L]] <- rlang::inject(arrow::schema(!!!fields))
  }

  list(
    in_types = in_types,
    out_types = rep(list(out_type), length(in_types)),
    mode = geoarrow_udf_wkb_mode(accepts, names(prototypes))
  )
}

#' How a kernel should turn a WKB argument into geometry
#'
#' Parsing WKB is the expensive part, so this picks the route that parses each
#' geometry once. A function that takes every geometry type gets the WKB array
#' untouched and parses it itself. One that takes exactly one type gets a
#' direct cast to that type. Anything in between has to infer the type first,
#' which costs a second pass over the column.
#'
#' @noRd
geoarrow_udf_wkb_mode <- function(accepts, all_types) {
  if (setequal(accepts, all_types)) {
    "pass"
  } else if (length(accepts) == 1) {
    tolower(accepts)
  } else {
    "downcast"
  }
}

#' Work out the kernels one function can offer
#'
#' Runs the function against zero length arrays of every combination of
#' geometry type its geometry arguments could take, and keeps the combinations
#' that succeed. A function that cannot handle a type simply has no kernel for
#' it.
#'
#' @noRd
geoarrow_udf_kernels <- function(fun, spec, schemas) {
  kinds <- geoarrow_udf_kind(spec)
  probes <- geoarrow_udf_probe(spec)
  slots <- which(kinds == "geometry")

  grid <- expand.grid(
    rep(list(names(schemas)), length(slots)),
    stringsAsFactors = FALSE
  )

  in_types <- list()
  out_types <- list()

  for (row in seq_len(nrow(grid))) {
    types <- as.character(grid[row, ])
    values <- Map(geoarrow_udf_value, kinds, probes, MoreArgs = list(NULL))
    fields <- lapply(kinds, geoarrow_udf_type, geometry_type = NULL)
    names(fields) <- names(spec)

    for (i in seq_along(slots)) {
      values[[slots[i]]] <- schemas[[types[i]]]$empty
      fields[[slots[i]]] <- schemas[[types[i]]]$type
    }

    out <- rlang::try_fetch(
      arrow::as_arrow_array(geoarrow_udf_normalise(
        rlang::inject(fun(!!!unname(values)))
      )),
      error = function(cnd) NULL
    )
    if (is.null(out)) {
      next
    }

    in_types[[length(in_types) + 1L]] <- rlang::inject(arrow::schema(!!!fields))
    out_types[[length(out_types) + 1L]] <- out$type
  }

  list(in_types = in_types, out_types = out_types)
}

#' The casts whose target type is fixed by their name
#'
#' These are the only geometry producing functions Arrow can run, because the
#' result type does not depend on what the column holds.
#'
#' @noRd
geoarrow_udf_cast_targets <- function() {
  c(
    ga_as_point = "point",
    ga_as_linestring = "linestring",
    ga_as_polygon = "polygon",
    ga_as_multipoint = "multipoint",
    ga_as_multilinestring = "multilinestring",
    ga_as_multipolygon = "multipolygon"
  )
}

#' A zero length array of one of the WKB storage types
#' @noRd
geoarrow_udf_empty_wkb <- function(type) {
  nanoarrow::as_nanoarrow_array(arrow::Array$create(list(), type = type))
}

#' Register one WKB to native cast
#'
#' The output type is read off the function itself rather than built from
#' geoarrow R, so it carries exactly the metadata encoding the Rust writes.
#'
#' @noRd
geoarrow_udf_register_cast <- function(name, prefix) {
  fun <- get(name, envir = asNamespace("geoarrowrs"))
  wkb <- geoarrow_udf_wkb_types()

  out <- rlang::try_fetch(
    arrow::as_arrow_array(geoarrow_udf_normalise(
      fun(geoarrow_udf_empty_wkb(wkb$binary))
    )),
    error = function(cnd) NULL
  )
  if (is.null(out)) {
    return(NULL)
  }

  in_types <- lapply(wkb, function(type) arrow::schema(x = type))
  arrow::register_scalar_function(
    paste0(prefix, name),
    local({
      f <- fun
      function(context, x) {
        arrow::as_arrow_array(geoarrow_udf_normalise(
          f(nanoarrow::as_nanoarrow_array(x))
        ))
      }
    }),
    in_type = unname(in_types),
    out_type = out$type,
    auto_convert = FALSE
  )
  paste0(prefix, name)
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
#' Registration is per CRS, not per table. Every GeoArrow geometry type gets
#' its own kernel, so one call covers points, linestrings, polygons, and their
#' multi variants at once. A function that cannot handle a type, such as
#' [ga_length_euclidean()] against polygons, simply has no kernel for it.
#'
#' Arguments that are not geometry are registered too. Numeric arguments such
#' as the `epsilon` of [ga_simplify()] take a `double` column or a literal and
#' are recycled row by row. Option arguments such as the `metric` of
#' [ga_densify()] take a literal string. Since Arrow has no optional arguments,
#' every argument of a registered function must be given.
#'
#' [ga_unary_union()], [ga_explode()], and [ga_flatten()] are not registered,
#' because they change the length of the array and a scalar kernel may not.
#'
#' A bare `binary` column of WKB, which is what plain Parquet writes, is
#' registered for too. The kernel parses it, and a geometry result comes back
#' as WKB, since a kernel's output type cannot depend on what the column turns
#' out to hold. To work on native GeoArrow instead, cast once with
#' [ga_as_point()] or one of its siblings and follow it with `compute()`.
#' Without the `compute()` Arrow inlines the cast into each expression that
#' reads it, so it runs once per use rather than once per column.
#'
#' A GeoArrow type carries its CRS, and Arrow compares that when it looks for a
#' kernel, so `crs` has to match the data. Pass every CRS you need in one call:
#' registering a name again replaces the kernels it had.
#'
#' Loading geoarrowrs calls this once for you with `crs = NULL`, so data with
#' no CRS works without any setup. Data that carries one still needs a call
#' naming it. Set `options(geoarrowrs.register_udfs = FALSE)` before loading to
#' skip that, which also stops the package pulling in arrow at load time.
#'
#' @param crs the coordinate reference system the kernels dispatch on. `NULL`,
#'   the default, registers for data with no CRS. Also accepts a CRS string, or
#'   any object carrying one: an Arrow `Table`, `RecordBatch`, `Dataset`,
#'   `Schema`, `Array`, or a GeoArrow array or vector. Give a list to cover
#'   several at once
#' @param functions names of geoarrowrs functions to register. `NULL`, the
#'   default, registers everything that can be
#' @param prefix prepended to each registered name, to avoid masking an
#'   existing Arrow binding
#' @returns the registered names, invisibly
#' @export
#' @examplesIf requireNamespace("arrow", quietly = TRUE) && requireNamespace("dplyr", quietly = TRUE) && requireNamespace("geoarrow", quietly = TRUE) && requireNamespace("sf", quietly = TRUE) && isTRUE(arrow::arrow_info()$capabilities[["acero"]])
#' library(dplyr)
#'
#' nc <- as.data.frame(read_shapefile(
#'   system.file("shape/nc.shp", package = "sf")
#' ))
#' tbl <- arrow::arrow_table(nc)
#'
#' register_geoarrow_udfs(crs = tbl)
#'
#' tbl |>
#'   mutate(area = ga_unsigned_area(geometry)) |>
#'   select(NAME, area) |>
#'   head(3) |>
#'   collect()
register_geoarrow_udfs <- function(crs = NULL, functions = NULL, prefix = "") {
  rlang::check_installed(
    c("arrow", "dplyr", "geoarrow", "nanoarrow"),
    "to register Arrow kernels."
  )

  if (!rlang::is_string(prefix)) {
    cli::cli_abort("{.arg prefix} must be a single string.")
  }

  catalogue <- geoarrow_udf_catalogue()
  casts <- names(geoarrow_udf_cast_targets())
  if (is.null(functions)) {
    functions <- c(names(catalogue), casts)
  }

  unknown <- setdiff(functions, c(names(catalogue), casts))
  if (length(unknown) > 0) {
    cli::cli_abort(c(
      "Cannot register {.fn {unknown}}.",
      i = "See {.help register_geoarrow_udfs} for what can be registered."
    ))
  }

  sets <- lapply(geoarrow_udf_metadatas(crs), geoarrow_udf_schemas)

  registered <- character()
  failed <- character()

  for (name in setdiff(functions, casts)) {
    fun <- get(name, envir = asNamespace("geoarrowrs"))
    spec <- catalogue[[name]]
    kernels <- lapply(sets, function(set) {
      geoarrow_udf_kernels(fun, spec, set)
    })
    wkb <- geoarrow_udf_wkb_kernels(fun, spec)
    kernels[[length(kernels) + 1L]] <- wkb

    in_types <- unlist(lapply(kernels, `[[`, "in_types"), recursive = FALSE)
    out_types <- unlist(lapply(kernels, `[[`, "out_types"), recursive = FALSE)

    if (length(in_types) == 0) {
      failed <- c(failed, name)
      next
    }

    arrow::register_scalar_function(
      paste0(prefix, name),
      geoarrow_udf_kernel(fun, geoarrow_udf_kind(spec), wkb$mode),
      in_type = in_types,
      out_type = out_types,
      auto_convert = FALSE
    )
    registered <- c(registered, paste0(prefix, name))
  }

  for (name in intersect(functions, names(geoarrow_udf_cast_targets()))) {
    registered <- c(registered, geoarrow_udf_register_cast(name, prefix))
  }

  if (length(failed) > 0) {
    cli::cli_warn(c(
      "Could not register {length(failed)} function{?s}.",
      i = "{.fn {failed}}"
    ))
  }

  invisible(registered)
}
