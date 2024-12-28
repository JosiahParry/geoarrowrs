mod area;
pub use area::*;
mod length;
pub use length::*;
mod bounding_rect;
pub use bounding_rect::*;

extendr_api::extendr_module! {
    mod algorithm;
    use area;
    use length;
    use bounding_rect;
}
