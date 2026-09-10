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
    "is_valid",
    "validation_error",
    "dimension",
    "boundary_dimension",
    "winding_order",
    "is_ccw",
    "is_cw",
    "remove_repeated_points",
    "to_degrees",
    "to_radians",
    "triangulate_earcut",
    "downcast_geometry",
    "coords",
    "exterior_coords",
    "n_coords",
    "lines",
    "self_intersections"
  )

  binary <- c(
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
    "line_intersection",
    "boolean_intersection",
    "boolean_union",
    "boolean_difference",
    "boolean_xor",
    "bearing_euclidean",
    "bearing_geodesic",
    "bearing_haversine",
    "bearing_rhumb",
    "dist_euclidean_pairwise",
    "dist_frechet_pairwise",
    "dist_geodesic_pairwise",
    "dist_hausdorff_pairwise",
    "dist_haversine_pairwise",
    "dist_rhumb_pairwise",
    "dist_vincenty_pairwise"
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
      simplify = c(geometry = "geometry", epsilon = "array"),
      simplify_vw = c(geometry = "geometry", epsilon = "array"),
      simplify_vw_preserve = c(geometry = "geometry", epsilon = "array"),
      simplify_idx = c(geometry = "geometry", epsilon = "array"),
      simplify_vw_idx = c(geometry = "geometry", epsilon = "array"),
      rotate_around_center = c(geometry = "geometry", degrees = "array"),
      rotate_around_centroid = c(geometry = "geometry", degrees = "array"),
      skew = c(geometry = "geometry", degrees = "array"),
      translate = c(
        geometry = "geometry",
        x_offset = "array",
        y_offset = "array"
      ),
      scale_xy = c(
        geometry = "geometry",
        x_factor = "array",
        y_factor = "array"
      ),
      skew_xy = c(
        geometry = "geometry",
        degrees_x = "array",
        degrees_y = "array"
      ),
      affine_transform = c(
        geometry = "geometry",
        a = "array",
        b = "array",
        xoff = "array",
        d = "array",
        e = "array",
        yoff = "array"
      ),
      concave_hull = c(
        geometry = "geometry",
        concavity = "array",
        length_threshold = "array"
      ),
      dbscan = c(geometry = "geometry", eps = "array", min_points = "array"),
      kmeans = c(geometry = "geometry", k = "array"),
      outlier_scores = c(geometry = "geometry", k_neighbours = "array"),
      chaikin_smoothing = c(geometry = "geometry", n_iterations = "int=1"),
      line_segmentize = c(geometry = "geometry", segment_count = "int=2"),
      line_segmentize_haversine = c(
        geometry = "geometry",
        segment_count = "int=2"
      ),
      densify = c(
        geometry = "geometry",
        max_segment_length = "array",
        metric = "string=euclidean"
      ),
      buffer = c(
        geometry = "geometry",
        distance = "array",
        line_cap = "string=round",
        line_join = "string=round",
        miter_limit = "num=2",
        round_segments = "num=8"
      ),
      triangulate_delaunay = c(
        x = "geometry",
        constrained = "bool=TRUE",
        snap_radius = "array"
      ),
      orient = c(geometry = "geometry", direction = "string=default"),
      interpolate_point = c(
        line = "geometry",
        value = "array",
        metric = "string=euclidean",
        measure = "string=ratio",
        from = "string=start"
      ),
      dest_euclidean = c(
        origin = "geometry",
        bearing = "array",
        distance = "array"
      ),
      dest_haversine = c(
        origin = "geometry",
        bearing = "array",
        distance = "array"
      ),
      dest_geodesic = c(
        origin = "geometry",
        bearing = "array",
        distance = "array"
      ),
      dest_rhumb = c(
        origin = "geometry",
        bearing = "array",
        distance = "array"
      ),
      point_at_distance_between = c(
        start = "geometry",
        end = "geometry",
        distance = "array",
        metric = "string=euclidean"
      ),
      point_at_ratio_between = c(
        start = "geometry",
        end = "geometry",
        ratio = "array",
        metric = "string=euclidean"
      ),
      points_along_line = c(
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

  schema <- try(nanoarrow::as_nanoarrow_schema(crs), silent = TRUE)
  if (inherits(schema, "try-error")) {
    schema <- nanoarrow::infer_nanoarrow_schema(crs)
  }
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

#' Turn the arguments Arrow hands a kernel into what the function expects
#' @noRd
geoarrow_udf_coerce <- function(args, kinds) {
  Map(
    function(arg, kind) {
      if (kind %in% c("geometry", "array")) {
        nanoarrow::as_nanoarrow_array(arg)
      } else {
        as.vector(arg)
      }
    },
    args,
    kinds
  )
}

#' Wrap a geoarrowrs function so Arrow can call it
#' @noRd
geoarrow_udf_kernel <- function(fun, kinds) {
  force(fun)
  force(kinds)
  function(context, ...) {
    arrow::as_arrow_array(do.call(fun, geoarrow_udf_coerce(list(...), kinds)))
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

    out <- try(
      arrow::as_arrow_array(do.call(fun, unname(values))),
      silent = TRUE
    )
    if (inherits(out, "try-error")) {
      next
    }

    in_types[[length(in_types) + 1L]] <- do.call(arrow::schema, fields)
    out_types[[length(out_types) + 1L]] <- out$type
  }

  list(in_types = in_types, out_types = out_types)
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
#' [length_euclidean()] against polygons, simply has no kernel for it.
#'
#' Arguments that are not geometry are registered too. Numeric arguments such
#' as the `epsilon` of [simplify()] take a `double` column or a literal and are
#' recycled row by row. Option arguments such as the `metric` of [densify()]
#' take a literal string. Since Arrow has no optional arguments, every argument
#' of a registered function must be given.
#'
#' [unary_union()], [explode()], and [flatten()] are not registered, because
#' they change the length of the array and a scalar kernel may not.
#'
#' A GeoArrow type carries its CRS, and Arrow compares that when it looks for a
#' kernel, so `crs` has to match the data. Pass every CRS you need in one call:
#' registering a name again replaces the kernels it had.
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
#'   mutate(area = unsigned_area(geometry)) |>
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
  if (is.null(functions)) {
    functions <- names(catalogue)
  }

  unknown <- setdiff(functions, names(catalogue))
  if (length(unknown) > 0) {
    cli::cli_abort(c(
      "Cannot register {.fn {unknown}}.",
      i = "See {.help register_geoarrow_udfs} for what can be registered."
    ))
  }

  sets <- lapply(geoarrow_udf_metadatas(crs), geoarrow_udf_schemas)

  registered <- character()
  failed <- character()

  for (name in functions) {
    fun <- getExportedValue("geoarrowrs", name)
    spec <- catalogue[[name]]
    kernels <- lapply(sets, function(set) {
      geoarrow_udf_kernels(fun, spec, set)
    })

    in_types <- unlist(lapply(kernels, `[[`, "in_types"), recursive = FALSE)
    out_types <- unlist(lapply(kernels, `[[`, "out_types"), recursive = FALSE)

    if (length(in_types) == 0) {
      failed <- c(failed, name)
      next
    }

    arrow::register_scalar_function(
      paste0(prefix, name),
      geoarrow_udf_kernel(fun, geoarrow_udf_kind(spec)),
      in_type = in_types,
      out_type = out_types,
      auto_convert = FALSE
    )
    registered <- c(registered, paste0(prefix, name))
  }

  if (length(failed) > 0) {
    cli::cli_warn(c(
      "Could not register {length(failed)} function{?s}.",
      i = "{.fn {failed}}"
    ))
  }

  invisible(registered)
}
