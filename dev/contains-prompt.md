I need help understanding and this trait and using it. 
The trait is defined as such: 

```rust
pub trait Contains<Rhs = Self> {
    // Required method
    fn contains(&self, rhs: &Rhs) -> BooleanArray;
}
```

It has the following implementations

```
impl<'a> Contains for LineStringArray
impl<'a> Contains for MultiLineStringArray
impl<'a> Contains for MultiPointArray
impl<'a> Contains for MultiPolygonArray
impl<'a> Contains for PointArray
impl<'a> Contains for PolygonArray
impl<'a> Contains<GeometryArray> for LineStringArray
impl<'a> Contains<GeometryArray> for MultiLineStringArray
impl<'a> Contains<GeometryArray> for MultiPointArray
impl<'a> Contains<GeometryArray> for MultiPolygonArray
impl<'a> Contains<GeometryArray> for PointArray
impl<'a> Contains<GeometryArray> for PolygonArray
impl<'a> Contains<LineStringArray> for MultiLineStringArray
impl<'a> Contains<LineStringArray> for MultiPointArray
impl<'a> Contains<LineStringArray> for MultiPolygonArray
impl<'a> Contains<LineStringArray> for PointArray
impl<'a> Contains<LineStringArray> for PolygonArray
impl<'a> Contains<MultiLineStringArray> for LineStringArray
impl<'a> Contains<MultiLineStringArray> for MultiPointArray
impl<'a> Contains<MultiLineStringArray> for MultiPolygonArray
impl<'a> Contains<MultiLineStringArray> for PointArray
impl<'a> Contains<MultiLineStringArray> for PolygonArray
impl<'a> Contains<MultiPointArray> for LineStringArray
impl<'a> Contains<MultiPointArray> for MultiLineStringArray
impl<'a> Contains<MultiPointArray> for MultiPolygonArray
impl<'a> Contains<MultiPointArray> for PointArray
impl<'a> Contains<MultiPointArray> for PolygonArray
impl<'a> Contains<MultiPolygonArray> for LineStringArray
impl<'a> Contains<MultiPolygonArray> for MultiLineStringArray
impl<'a> Contains<MultiPolygonArray> for MultiPointArray
impl<'a> Contains<MultiPolygonArray> for PointArray
impl<'a> Contains<MultiPolygonArray> for PolygonArray
impl<'a> Contains<PointArray> for LineStringArray
impl<'a> Contains<PointArray> for MultiLineStringArray
impl<'a> Contains<PointArray> for MultiPointArray
impl<'a> Contains<PointArray> for MultiPolygonArray
impl<'a> Contains<PointArray> for PolygonArray
impl<'a> Contains<PolygonArray> for LineStringArray
impl<'a> Contains<PolygonArray> for MultiLineStringArray
impl<'a> Contains<PolygonArray> for MultiPointArray
impl<'a> Contains<PolygonArray> for MultiPolygonArray
impl<'a> Contains<PolygonArray> for PointArray
impl<'a> Contains<RectArray> for LineStringArray
impl<'a> Contains<RectArray> for MultiLineStringArray
impl<'a> Contains<RectArray> for MultiPointArray
impl<'a> Contains<RectArray> for MultiPolygonArray
impl<'a> Contains<RectArray> for PointArray
impl<'a> Contains<RectArray> for PolygonArray
```

I have two ChunkedGeometryArray<NativeArrayDyn>s. I have zipped these into two iterators.

```rust
lhs.chunks().into_iter().zip(rhs.chunks()).map(|(lhsi, rhsi)| {
 // i need help here
});
```

Each item in the iterator is a `&NativeArrayDyn`. I can cast them to `&dyn NativeArray` with `.as_ref()`.
From here I can fallibly cast them to each type by using, for example `as_geometry_opt()`, `as_polygon_opt()` etc.

I can figure out the type of the left hand array and the right hand array using their data  type.
```rust
let ldt = lhs.data_type();
let rdt = rhs.data_type();
```
Which is defined as 
```rust
pub enum NativeType {
    Point(CoordType, Dimension),
    LineString(CoordType, Dimension),
    Polygon(CoordType, Dimension),
    MultiPoint(CoordType, Dimension),
    MultiLineString(CoordType, Dimension),
    MultiPolygon(CoordType, Dimension),
    GeometryCollection(CoordType, Dimension),
    Rect(Dimension),
    Geometry(CoordType),
}
```
we can ignore the `CoordType` and `Dimension` for this. 

Can you help me finish my function so that iterator calls `contains()` for each element? Returning `Error::Other("Incompatible geometry types".to_string())` 
if there is an incompatible pair between `x` and `y`? 

```rust
#[extendr]
pub fn contains_(x: GeoChunks, y: GeoChunks) -> Result<BooleanChunks> {
    let lhs = x.0;
    let rhs = y.0;

    let ldt = lhs.data_type();
    let rdt = rhs.data_type();

    let results = lhs
        .chunks()
        .into_iter()
        .zip(rhs.chunks())
        .map(|(lhsi, rhsi)| {
            let lhs_dyn = lhsi.as_ref();
            let rhs_dyn = rhsi.as_ref();

            let resi = match (ldt, rdt) {
                (NativeType::Point(..), NativeType::Point(..)) => {
                    let lhs_array = lhs_dyn
                        .as_point_opt()
                        .ok_or_else(|| Error::Other("Expected PointArray for lhs".to_string()))?;
                    let rhs_array = rhs_dyn
                        .as_point_opt()
                        .ok_or_else(|| Error::Other("Expected PointArray for rhs".to_string()))?;
                    Ok(lhs_array.contains(rhs_array))
                }
                (NativeType::LineString(..), NativeType::LineString(..)) => {
                    let lhs_array = lhs_dyn.as_line_string_opt().ok_or_else(|| {
                        Error::Other("Expected LineStringArray for lhs".to_string())
                    })?;
                    let rhs_array = rhs_dyn.as_line_string_opt().ok_or_else(|| {
                        Error::Other("Expected LineStringArray for rhs".to_string())
                    })?;
                    Ok(lhs_array.contains(rhs_array))
                }
                (NativeType::Polygon(..), NativeType::Polygon(..)) => {
                    let lhs_array = lhs_dyn
                        .as_polygon_opt()
                        .ok_or_else(|| Error::Other("Expected PolygonArray for lhs".to_string()))?;
                    let rhs_array = rhs_dyn
                        .as_polygon_opt()
                        .ok_or_else(|| Error::Other("Expected PolygonArray for rhs".to_string()))?;
                    Ok(lhs_array.contains(rhs_array))
                }
                // Add more match arms for the other compatible types...
                _ => Err(Error::Other("Incompatible geometry types".to_string())),
            };
            resi
        })
        .collect::<Result<Vec<_>>>()
        .handle_error()?;

    let res = ChunkedArray::new(results);
    Ok(BooleanChunks(res))
}

```