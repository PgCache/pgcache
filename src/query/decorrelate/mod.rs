use error_set::error_set;
use rootcause::Report;

mod bound;

#[cfg(test)]
mod tests;

pub use bound::query_decorrelate;

error_set! {
    DecorrelateError := {
        #[display("Non-decorrelatable correlated subquery: {reason}")]
        NonDecorrelatable { reason: String },
    }
}

pub type DecorrelateResult<T> = Result<T, Report<DecorrelateError>>;
