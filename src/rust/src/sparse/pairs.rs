use arrow::array::{Array, ArrayData, ListArray, StructArray, UInt32Array, UInt32Builder};
use arrow::datatypes::{DataType, Field};
use arrow_extendr::{FromArrowRobj, IntoArrowRobj};
use extendr_api::prelude::*;
use std::sync::Arc;

/// Expand a sparse predicate into the row pairs a join needs
///
/// Turns the list of matches each sparse predicate returns into two columns,
/// one row per pair, which is the shape [ga_join()] takes and the shape a
/// database returns a spatial join in.
///
/// @details
/// Everything but the padding is expressible with Arrow's own `list_flatten()`
/// and `list_parent_indices()`. What they cannot do is `left = TRUE`, which has
/// to put back a row for each row of `x` that matched nothing, so the whole
/// expansion happens here rather than half here and half in R.
///
/// Rows are 1 based, matching what the sparse predicates return. A row of `x`
/// that matched nothing appears once with a null `y` when `left = TRUE`, and
/// not at all otherwise.
///
/// @param hits a list array from one of the sparse predicates
/// @param left whether to keep the rows of `x` that matched nothing
/// @returns a struct array of `x` and `y` row numbers
/// @export
/// @family topology
/// @examplesIf requireNamespace("geoarrow", quietly = TRUE)
/// x <- ga_xy(c(0, 5, 1), c(0, 5, 1))
/// y <- ga_xy(c(0, 1), c(0, 1))
///
/// # the middle row of x matches nothing, so it is only there with left = TRUE
/// ga_sparse_pairs(ga_sparse_intersects(x, y))
/// ga_sparse_pairs(ga_sparse_intersects(x, y), left = TRUE)
#[extendr]
fn ga_sparse_pairs(
    hits: Robj,
    #[extendr(default = "FALSE")] left: bool,
) -> extendr_api::Result<Robj> {
    let data = ArrayData::from_arrow_robj(&hits).map_err(|e| Error::Other(e.to_string()))?;
    if !matches!(data.data_type(), DataType::List(_)) {
        return Err(Error::Other(
            "`hits` must be the list array a sparse predicate returns".into(),
        ));
    }

    let list = ListArray::from(data);
    let values = list
        .values()
        .as_any()
        .downcast_ref::<UInt32Array>()
        .ok_or_else(|| Error::Other("`hits` must hold row numbers".into()))?;
    let offsets = list.value_offsets();

    let mut xs = UInt32Builder::new();
    let mut ys = UInt32Builder::new();

    for row in 0..list.len() {
        let start = offsets[row] as usize;
        let end = offsets[row + 1] as usize;

        // a null element is a row with no box to search with, which matched nothing
        if list.is_null(row) || start == end {
            if left {
                xs.append_value(row as u32 + 1);
                ys.append_null();
            }
            continue;
        }

        for slot in start..end {
            xs.append_value(row as u32 + 1);
            ys.append_value(values.value(slot));
        }
    }

    StructArray::try_new(
        vec![
            Arc::new(Field::new("x", DataType::UInt32, false)),
            Arc::new(Field::new("y", DataType::UInt32, true)),
        ]
        .into(),
        vec![Arc::new(xs.finish()), Arc::new(ys.finish())],
        None,
    )
    .map_err(|e| Error::Other(e.to_string()))?
    .into_data()
    .into_arrow_robj()
}

extendr_module! {
    mod pairs;
    fn ga_sparse_pairs;
}
