use error_set::error_set;
use rootcause::Report;

mod bound;
mod tree;

#[cfg(test)]
mod tests;

pub use bound::query_decorrelate;
pub use tree::query_decorrelate_tree;

error_set! {
    DecorrelateError := {
        #[display("Non-decorrelatable correlated subquery: {reason}")]
        NonDecorrelatable { reason: String },
    }
}

pub type DecorrelateResult<T> = Result<T, Report<DecorrelateError>>;
