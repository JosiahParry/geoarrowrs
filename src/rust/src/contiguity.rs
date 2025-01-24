use extendr_api::prelude::*;
use geoarrow::algorithm::native::bounding_rect::bounding_rect_geometry;
use geoarrow::array::{AsNativeArray, MultiPolygonArray};
use geoarrow::chunked_array::{ChunkedMultiPolygonArray, ChunkedPolygonArray};
use geoarrow::datatypes::NativeType;
use geoarrow::ArrayBase;
use std::collections::HashSet;

use crate::ffi::GeoChunks;
use geo::Relate;
use geo_index::rtree::RTreeIndex;
use geoarrow::algorithm::native::Concatenate;
use geoarrow::trait_::NativeScalar;
use geoarrow::{array::PolygonArray, NativeArray};

pub enum ContiguityCase {
    QueenStrict,
    Queen,
    Rook,
    RookStrict,
}

pub trait ContiguityGeometry {}

impl ContiguityGeometry for MultiPolygonArray {}
impl ContiguityGeometry for PolygonArray {}

fn process_candidates<T, F>(
    i: usize,
    candidates: Vec<usize>,
    geoms: &T,
    seen_pairs: &mut HashSet<(usize, usize)>,
) where
    T: for<'a> geoarrow::trait_::ArrayAccessor<'a> + ArrayBase,
    for<'a> <T as geoarrow::trait_::ArrayAccessor<'a>>::ItemGeo: geo::Relate<F>,
    F: geo::GeoFloat,
{
    for j in candidates {
        if i == j {
            continue;
        }

        let mut pair = [i, j];
        pair.sort();
        let pair = (pair[0], pair[1]);

        if seen_pairs.contains(&pair) {
            continue;
        }

        if let (Some(geom_i), Some(geom_j)) = (geoms.get_as_geo(i), geoms.get_as_geo(j)) {
            let relation = geom_i.relate(&geom_j);

            if relation.is_touches() {
                seen_pairs.insert(pair);
            }
        }
    }
}

// NOTE to handle
pub fn contiguity_<T, F>(geoms: &T, node_size: usize) -> (Vec<usize>, Vec<usize>)
where
    T: for<'a> geoarrow::trait_::ArrayAccessor<'a>
        + ArrayBase
        + geoarrow::algorithm::geo_index::RTree,
    <T as geoarrow::algorithm::geo_index::RTree>::Output: RTreeIndex<f64>,
    for<'a> <T as geoarrow::trait_::ArrayAccessor<'a>>::ItemGeo: geo::Relate<F>,
    F: geo::GeoFloat,
{
    let tree = geoms.create_rtree_with_node_size(node_size);

    let mut seen_pairs = HashSet::new();

    for (i, poly) in geoms.iter().enumerate() {
        let p = poly.unwrap();
        let (min, max) = bounding_rect_geometry(&p.to_geo_geometry());
        let candidates = tree.search(min[0], min[1], max[0], max[1]);
        process_candidates(i, candidates, geoms, &mut seen_pairs);
    }

    let mut nbs = Vec::new();
    for (i, j) in &seen_pairs {
        nbs.push((i, j));
        nbs.push((j, i));
    }

    nbs.sort();

    let res: (Vec<usize>, Vec<usize>) = nbs.into_iter().unzip();
    res
}

#[extendr]
pub fn contiguity(geoms: GeoChunks) -> Robj {
    let nt = geoms.0.data_type();
    let (from, to) = match nt {
        NativeType::Polygon(..) => {
            let geoms = geoms
                .0
                .chunks()
                .iter()
                .map(|ci| ci.as_ref().as_polygon().clone())
                .collect::<Vec<_>>();

            let concatted = ChunkedPolygonArray::new(geoms).concatenate().unwrap();

            contiguity_(&concatted, 10)
        }
        NativeType::MultiPolygon(..) => {
            let geoms = geoms
                .0
                .chunks()
                .iter()
                .map(|ci| ci.as_ref().as_multi_polygon().clone())
                .collect::<Vec<_>>();

            let concatted = ChunkedMultiPolygonArray::new(geoms).concatenate().unwrap();
            contiguity_(&concatted, 10)
        }
        _ => throw_r_error("Unsupported geometry type"),
    };

    data_frame!(
        from = from
            .into_iter()
            .map(|i| Rint::from(i as i32))
            .collect::<Integers>(),
        to = to
            .into_iter()
            .map(|i| Rint::from(i as i32))
            .collect::<Integers>()
    )
}

extendr_module! {
    mod contiguity;
    fn contiguity;
}
