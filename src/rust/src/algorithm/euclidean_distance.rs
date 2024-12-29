use arrow::{array::Float64Builder, datatypes::Float64Type};
use extendr_api::prelude::*;
use geo::{Distance, Euclidean};
use geoarrow::{
    algorithm::native::Cast,
    array::{AsNativeArray, CoordType},
    chunked_array::ChunkedArray,
    datatypes::NativeType::{self},
    trait_::ArrayAccessor,
    ArrayBase, NativeArray,
};

use crate::{
    ffi::{GeoChunks, PrimitiveChunks},
    HandleError,
};

#[extendr]
pub fn distance_euclidean_pairwise_(
    x: GeoChunks,
    y: GeoChunks,
) -> Result<PrimitiveChunks<Float64Type>> {
    let res =
        x.0.chunks()
            .into_iter()
            .zip(y.0.chunks())
            .map(|(lhs, rhs)| {
                assert_eq!(lhs.len(), rhs.len());
                let mut output_array = Float64Builder::with_capacity(lhs.len());
                let lhs_binding = lhs.as_ref();
                let rhs_binding = rhs.as_ref();

                let lhs_binding = lhs_binding
                    .cast(NativeType::Geometry(CoordType::Separated))
                    .handle_error()?;

                let lhs_binding = lhs_binding.as_ref();
                let lhs_geom = lhs_binding.as_geometry();

                let rhs_binding = rhs_binding
                    .cast(NativeType::Geometry(CoordType::Separated))
                    .handle_error()?;

                let rhs_binding = rhs_binding.as_ref();
                let rhs_geom = rhs_binding.as_geometry();

                let inner_iter = lhs_geom.iter_geo().zip(rhs_geom.iter_geo());

                for item in inner_iter {
                    if let (Some(l), Some(r)) = item {
                        output_array.append_value(Euclidean::distance(&l, &r));
                    } else {
                        output_array.append_null();
                    }
                }
                let res = output_array.finish();
                Ok(res)
            })
            .collect::<Result<Vec<_>>>()
            .handle_error()?;
    Ok(PrimitiveChunks(ChunkedArray::new(res)))
}

extendr_module! {
    mod euclidean_distance;
    fn distance_euclidean_pairwise_;
}
