mod buffer;
mod centroid;
mod chaikin;
mod repeated_points;
mod segmentize;
use extendr_api::prelude::*;

extendr_module! {
    mod misc;
    use buffer;
    use centroid;
    use chaikin;
    use repeated_points;
    use segmentize;
}
