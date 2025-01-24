use extendr_api::prelude::*;
pub mod type_ids;

extendr_module! {
    mod native;
    use type_ids;
}
