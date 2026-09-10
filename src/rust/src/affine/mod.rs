mod matrix;
mod rotate;
mod scale;
mod skew;
mod translate;
use extendr_api::prelude::*;

extendr_module! {
    mod affine;
    use matrix;
    use rotate;
    use scale;
    use skew;
    use translate;
}
