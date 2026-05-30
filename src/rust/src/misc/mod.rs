mod buffer;
mod centroid;
mod repeated_points;
use extendr_api::prelude::*;

extendr_module! {
    mod misc;
    use buffer;
    use repeated_points;
    use centroid;
}
