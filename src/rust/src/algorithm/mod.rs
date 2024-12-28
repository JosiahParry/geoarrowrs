mod area;
pub use area::*;

extendr_api::extendr_module! {
    mod algorithm;
    use area;
}
