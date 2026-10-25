use arrow::array::{Array, ArrayData, ArrayRef, Float64Array, StructArray};
use arrow::buffer::NullBuffer;
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, Fields};
use arrow_extendr::{FromArrowRobj, IntoArrowRobj};
use extendr_api::prelude::*;
use geo::algorithm::bool_ops::BooleanOps;
use geo::{Area, BoundingRect, MultiPolygon};
use geo_index::rtree::RTreeIndex;
use rayon::prelude::*;
use std::sync::Arc;

use crate::sparse::{as_geoms_and_rects, as_multipolygon, index_rects};
use crate::threads::{Threads, with_pool};
use crate::{check_pair_len, try_float_array};

/// The part of a source inside a target: the source's row, its area, and its shape when `weights` needs it.
type Piece = (usize, f64, Option<MultiPolygon<f64>>);

/// Read `values` as float columns, with their names when they came as a table.
fn try_value_columns(
    values: Robj,
    n: usize,
) -> extendr_api::Result<(Vec<Float64Array>, Option<Vec<String>>)> {
    let non_numeric =
        |name: &str| Error::Other(format!("column `{name}` of `values` must be numeric"));

    let (columns, names) = match ArrayData::from_arrow_robj(&values) {
        Ok(data) if matches!(data.data_type(), DataType::Struct(_)) => {
            let table = StructArray::from(data);
            let mut columns = Vec::with_capacity(table.num_columns());
            for (field, column) in table.fields().iter().zip(table.columns()) {
                if !field.data_type().is_numeric() {
                    return Err(non_numeric(field.name()));
                }
                let floats = cast(column.as_ref(), &DataType::Float64)
                    .map_err(|e| Error::Other(e.to_string()))?;
                let floats = Float64Array::from(floats.to_data());
                // a null row of the table is null in every column
                let nulls = NullBuffer::union(table.nulls(), floats.nulls());
                columns.push(Float64Array::new(floats.values().clone(), nulls));
            }
            let names = table.fields().iter().map(|f| f.name().clone()).collect();
            (columns, Some(names))
        }
        Ok(_) => (vec![try_float_array(values, "values")?], None),
        Err(_) => match values.as_list() {
            Some(list) => {
                let mut columns = Vec::with_capacity(list.len());
                let mut names = Vec::with_capacity(list.len());
                for (name, column) in list.iter() {
                    if column.inherits("factor") {
                        return Err(non_numeric(name));
                    }
                    columns.push(try_float_array(column, "values").map_err(|_| non_numeric(name))?);
                    names.push(name.to_string());
                }
                (columns, Some(names))
            }
            None => (vec![try_float_array(values, "values")?], None),
        },
    };

    if columns.is_empty() {
        return Err(Error::Other(
            "`values` must have at least one column".into(),
        ));
    }
    for column in &columns {
        // a single value recycles over every source
        if column.len() != 1 {
            check_pair_len(column.len(), n, "values", "x")?;
        }
    }
    Ok((columns, names))
}

/// Interpolate values from one set of polygons onto another by area
///
/// Moves values that describe the polygons of `x` onto the polygons of `to`,
/// sharing each value out by the area the two overlap. This matches
/// `sf::st_interpolate_aw()`.
///
/// @details
/// Areal interpolation needs to know how each value relates to the area it
/// describes, which you state with `extensive`. For a spatially extensive
/// value, such as a count of people, `ga_interpolate_aw()` gives each target
/// the share of the value in proportion to the area it takes from the source,
/// so it preserves the total. For a spatially intensive value, such as a
/// density, `ga_interpolate_aw()` gives each target the mean of the values it
/// overlaps, weighted by area, so it preserves the mean.
///
/// With `a` the area that source `i` shares with target `j`,
/// `ga_interpolate_aw()` returns the sum over `i` of `v[i] * a / A[i]` for an
/// extensive column, where `A[i]` is the area of the source, and of
/// `v[i] * a / B[j]` for an intensive column, where `B[j]` is the area of the
/// target that the sources cover. `allocate_total = TRUE` takes `A[i]` as the
/// area of the source that the targets cover, so `ga_interpolate_aw()` hands
/// out all of each value even where the targets leave part of its source
/// uncovered, as tobler's `area_interpolate()` does by default.
/// `include_non_intersected = TRUE` takes `B[j]` as the whole area of the
/// target, as sf's argument of that name does.
///
/// With `weights`, `ga_interpolate_aw()` shares each source out by weight
/// rather than by area, as sf does for dasymetric mapping. It spreads the weight
/// of each target over its overlaps by area, then gives each overlap the
/// fraction of its source's weight that it holds. `ga_interpolate_aw()` takes
/// `weights` for extensive columns only.
///
/// `ga_interpolate_aw()` measures planar area, so project geographic
/// coordinates first. It reads polygons, multipolygons, and boxes, and treats
/// any other geometry as covering nothing. It counts a source as overlapping a
/// target only when their intersection has area, so it ignores polygons that
/// share just an edge.
///
/// `ga_interpolate_aw()` returns null for a target that no source overlaps, and
/// for a target that a source with a null value overlaps. With `na_rm = TRUE`,
/// it first drops each source with a null in any column, as sf's `na.rm` does.
/// When `values` is a table, `ga_interpolate_aw()` returns a struct array that
/// is null in each row no source overlaps.
///
/// @param x a GeoArrow polygon array that the values describe
/// @param values the values of `x`, one row per row of `x` or a single row for
///   all of them: a numeric vector or float array, or a data frame, list, or
///   struct array of numeric columns
/// @param to a GeoArrow polygon array to interpolate onto
/// @param extensive `TRUE` to interpolate a column by preserving its total,
///   `FALSE` to preserve its mean; length 1 or one per column of `values`
/// @param include_non_intersected whether to divide an intensive column by the
///   whole area of each target rather than the part the sources cover
/// @param allocate_total whether to divide an extensive column by the part of
///   each source the targets cover rather than its whole area
/// @param na_rm whether to drop each source with a null in any column of
///   `values`
/// @param weights the weight of each row of `to`, to share extensive values out
///   by in place of area, or `NULL`
/// @returns a float64 array of the same length as `to`, or a struct array of
///   float64 columns when `values` is a table
/// @export
/// @family aggregate
/// @references Pebesma, E., Fleischmann, M., Parry, J., et al. (2025). Spatial
///   data science languages: commonalities and needs. *Journal of Spatial
///   Information Science*, 31, 119--144.
///   \doi{10.5311/JOSIS.2025.31.462}
/// @examplesIf requireNamespace("sf", quietly = TRUE) && requireNamespace("geoarrow", quietly = TRUE)
/// nc <- sf::st_transform(
///   sf::read_sf(system.file("shape/nc.shp", package = "sf")),
///   32119
/// )
/// counties <- geoarrow::as_geoarrow_array(sf::st_geometry(nc))
/// cells <- ga_make_grid(counties, n = c(10, 5))
///
/// # births are a count, so the cells keep the state's total
/// births <- ga_interpolate_aw(counties, nc$BIR74, cells, extensive = TRUE)
/// sum(as.vector(births), na.rm = TRUE) / sum(nc$BIR74)
///
/// # a count and a rate together, each with its own support
/// both <- data.frame(births = nc$BIR74, sids_rate = nc$SID74 / nc$BIR74)
/// out <- ga_interpolate_aw(counties, both, cells, extensive = c(TRUE, FALSE))
/// head(na.omit(as.data.frame(out)))
#[extendr]
#[allow(clippy::too_many_arguments)]
fn ga_interpolate_aw(
    x: Robj,
    values: Robj,
    to: Robj,
    extensive: Robj,
    #[extendr(default = "FALSE")] include_non_intersected: bool,
    #[extendr(default = "FALSE")] allocate_total: bool,
    #[extendr(default = "FALSE")] na_rm: bool,
    #[extendr(default = "NULL")] weights: Robj,
) -> extendr_api::Result<Robj> {
    let threads = Threads::get();
    let (xs, x_rects) = as_geoms_and_rects(threads, x)?;
    let (ts, t_rects) = as_geoms_and_rects(threads, to)?;
    let (columns, names) = try_value_columns(values, xs.len())?;

    let flags = extensive
        .as_logical_vector()
        .filter(|flags| flags.iter().all(|f| !f.is_na()))
        .ok_or_else(|| Error::Other("`extensive` must be TRUE or FALSE, with no NA".into()))?;
    if flags.len() != 1 && flags.len() != columns.len() {
        return Err(Error::Other(format!(
            "`extensive` must be length 1 or one per column of `values` ({}), got {}",
            columns.len(),
            flags.len()
        )));
    }
    let is_extensive = |c: usize| flags[c % flags.len()].is_true();
    let any_extensive = (0..columns.len()).any(is_extensive);
    let all_extensive = (0..columns.len()).all(is_extensive);

    let weights = if weights.is_null() {
        None
    } else {
        let weights = try_float_array(weights, "weights")?;
        check_pair_len(weights.len(), ts.len(), "weights", "to")?;
        if !all_extensive || include_non_intersected || allocate_total {
            return Err(Error::Other(
                "`weights` shares out extensive columns only, so it needs every column extensive and `include_non_intersected` and `allocate_total` FALSE".into(),
            ));
        }
        Some(weights)
    };

    // a value recycled over the rows, or None where it is null
    let value = |column: &Float64Array, i: usize| {
        let at = i % column.len();
        (!column.is_null(at)).then(|| column.value(at))
    };

    let (sources, targets) = with_pool(threads, || {
        let sources = xs
            .par_iter()
            .enumerate()
            .map(|(i, g)| {
                if na_rm && columns.iter().any(|c| value(c, i).is_none()) {
                    return None;
                }
                g.as_ref().and_then(as_multipolygon)
            })
            .collect::<Vec<_>>();
        let targets = ts
            .par_iter()
            .map(|g| g.as_ref().and_then(as_multipolygon))
            .collect::<Vec<_>>();
        (sources, targets)
    });

    // every overlap with area, grouped by target and ordered by source as sf orders them
    let keep_shapes = weights.is_some();
    let pieces = match index_rects(&x_rects) {
        Some((tree, positions)) => with_pool(threads, || {
            targets
                .par_iter()
                .zip(t_rects.par_iter())
                .map(|(target, rect)| {
                    let (Some(target), Some(rect)) = (target, rect) else {
                        return Vec::new();
                    };
                    let mut found = tree
                        .search(rect.min().x, rect.min().y, rect.max().x, rect.max().y)
                        .into_iter()
                        .filter_map(|k| positions.get(k as usize).map(|i| *i as usize))
                        .filter_map(|i| {
                            let shape = sources[i].as_ref()?.intersection(&**target);
                            let area = shape.unsigned_area();
                            (area > 0.0).then(|| (i, area, keep_shapes.then_some(shape)))
                        })
                        .collect::<Vec<Piece>>();
                    found.sort_unstable_by_key(|piece| piece.0);
                    found
                })
                .collect::<Vec<_>>()
        }),
        None => (0..ts.len()).map(|_| Vec::new()).collect(),
    };

    let target_area = || {
        with_pool(threads, || {
            targets
                .par_iter()
                .map(|t| t.as_ref().map_or(0.0, |t| t.unsigned_area()))
                .collect::<Vec<_>>()
        })
    };

    // what an extensive overlap divides by: its source's area, or the part of it the targets cover
    let source_den = if !any_extensive || keep_shapes {
        Vec::new()
    } else if allocate_total {
        let mut covered = vec![0.0; xs.len()];
        for (i, area, _) in pieces.iter().flatten() {
            covered[*i] += area;
        }
        covered
    } else {
        with_pool(threads, || {
            sources
                .par_iter()
                .map(|s| s.as_ref().map_or(0.0, |s| s.unsigned_area()))
                .collect::<Vec<_>>()
        })
    };

    // what an intensive overlap divides by: the part of its target the sources cover, or its whole area
    let target_den = if all_extensive {
        Vec::new()
    } else if include_non_intersected {
        target_area()
    } else {
        pieces
            .iter()
            .map(|found| found.iter().map(|piece| piece.1).sum::<f64>())
            .collect()
    };

    // with weights, the share of its source each overlap gets: the target weight it holds over its source's total
    let shares = weights.as_ref().map(|weights| {
        let areas = target_area();
        let index = index_rects(&t_rects);
        let held = with_pool(threads, || {
            pieces
                .par_iter()
                .enumerate()
                .map(|(j, found)| {
                    found
                        .iter()
                        .map(|(_, area, shape)| {
                            let shape = shape.as_ref()?;
                            let rect = shape.bounding_rect()?;
                            let (tree, positions) = index.as_ref()?;
                            let mut over = tree
                                .search(rect.min().x, rect.min().y, rect.max().x, rect.max().y)
                                .into_iter()
                                .filter_map(|k| positions.get(k as usize).map(|t| *t as usize))
                                .collect::<Vec<_>>();
                            over.sort_unstable();
                            over.into_iter().try_fold(0.0, |acc, t| {
                                // the overlap lies inside its own target, so that intersection is the overlap itself
                                let shared = if t == j {
                                    *area
                                } else {
                                    targets[t].as_ref().map_or(0.0, |other| {
                                        shape.intersection(&**other).unsigned_area()
                                    })
                                };
                                if shared > 0.0 {
                                    Some(acc + value(weights, t)? * shared / areas[t])
                                } else {
                                    Some(acc)
                                }
                            })
                        })
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>()
        });

        let mut totals = vec![Some(0.0); xs.len()];
        for (found, held) in pieces.iter().zip(&held) {
            for ((i, _, _), w) in found.iter().zip(held) {
                totals[*i] = totals[*i].zip(*w).map(|(a, b)| a + b);
            }
        }
        held.into_iter()
            .zip(&pieces)
            .map(|(held, found)| {
                held.into_iter()
                    .zip(found)
                    .map(|(w, (i, _, _))| {
                        // sf gives NA to a source whose overlaps hold no weight
                        let total = totals[*i].filter(|t| *t != 0.0)?;
                        Some(w? / total)
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>()
    });

    let estimate = |c: usize| {
        let column = &columns[c];
        let extensive = is_extensive(c);
        let estimates = with_pool(threads, || {
            pieces
                .par_iter()
                .enumerate()
                .map(|(j, found)| {
                    if found.is_empty() {
                        return None;
                    }
                    found
                        .iter()
                        .enumerate()
                        .try_fold(0.0, |acc, (k, (i, area, _))| {
                            let v = value(column, *i)?;
                            let part = match &shares {
                                Some(shares) => v * shares[j][k]?,
                                None if extensive => v * area / source_den[*i],
                                None => v * area / target_den[j],
                            };
                            Some(acc + part)
                        })
                })
                .collect::<Vec<_>>()
        });
        Float64Array::from(estimates)
    };

    let Some(names) = names else {
        return estimate(0).into_arrow_robj();
    };

    let fields = names
        .iter()
        .map(|name| Field::new(name, DataType::Float64, true))
        .collect::<Fields>();
    let estimates = (0..columns.len())
        .map(|c| Arc::new(estimate(c)) as ArrayRef)
        .collect::<Vec<_>>();
    // a null row is a target that no source overlaps
    let covered = NullBuffer::from(
        pieces
            .iter()
            .map(|found| !found.is_empty())
            .collect::<Vec<_>>(),
    );

    StructArray::try_new(fields, estimates, Some(covered))
        .map_err(|e| Error::Other(e.to_string()))?
        .into_data()
        .into_arrow_robj()
}

extendr_module! {
    mod interpolate_area;
    fn ga_interpolate_aw;
}
