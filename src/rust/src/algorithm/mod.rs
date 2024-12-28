mod area;
pub use area::*;
mod length;
pub use length::*;
mod bounding_rect;
pub use bounding_rect::*;
mod center;
pub use center::*;
mod centroid;
pub use centroid::*;
mod chaikin_smoothing;
pub use chaikin_smoothing::*;

extendr_api::extendr_module! {
    mod algorithm;
    use area;
    use bounding_rect;
    use chaikin_smoothing;
    use center;
    use centroid;
    use length;
}
