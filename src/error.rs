//! implementation for [`SRCError`]
use std::error::Error;
use std::fmt;
use std::sync::Arc;

/// Error struct which makes it easy to know if the resulting error is also preserved in the cache
/// or not. And whether trying it again might not cause an error.
///
/// When the error was caused by another error, that error is available via [`Error::source`],
/// so it can be downcast, e.g. to a `reqwest::Error`, while `cause` holds its message.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct SRCError {
    pub error: String,
    pub cause: Option<String>,
    pub retriable: bool,
    pub cached: bool,
    /// Shared, so an error returned from the cache is a cheap copy.
    source: Option<Arc<dyn Error + Send + Sync>>,
}

/// Implements standard error so error handling can be simplified
impl Error for SRCError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.source.as_deref().map(|e| e as &(dyn Error + 'static))
    }
}

/// Compares everything but the source, which isn't comparable, and is described by `cause`.
impl PartialEq for SRCError {
    fn eq(&self, other: &SRCError) -> bool {
        self.error == other.error
            && self.cause == other.cause
            && self.retriable == other.retriable
            && self.cached == other.cached
    }
}

/// Gives the information from the error in a readable format.
impl fmt::Display for SRCError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.cause {
            Some(cause) => write!(
                f,
                "Error: {}, was cause by {}, it's retriable: {}, it's cached: {}",
                self.error, cause, self.retriable, self.cached
            ),
            None => write!(
                f,
                "Error: {} had no other cause, it's retriable: {}, it's cached: {}",
                self.error, self.retriable, self.cached
            ),
        }
    }
}

/// Specific error from which can be determined whether retrying might not lead to an error and
/// whether the error is cashed, it's turned into the cashed variant when it's put into the cache.
impl SRCError {
    pub fn new(error: &str, cause: Option<String>, retriable: bool) -> SRCError {
        SRCError {
            error: error.to_owned(),
            cause,
            retriable,
            cached: false,
            source: None,
        }
    }
    /// The error `cause` is kept as the [`Error::source`] of the new error.
    pub fn retryable_with_cause<T: Into<Box<dyn Error + Send + Sync>>>(
        cause: T,
        error: &str,
    ) -> SRCError {
        SRCError::with_source(error, cause.into(), true)
    }
    /// The error `cause` is kept as the [`Error::source`] of the new error.
    pub fn non_retryable_with_cause<T: Into<Box<dyn Error + Send + Sync>>>(
        cause: T,
        error: &str,
    ) -> SRCError {
        SRCError::with_source(error, cause.into(), false)
    }
    fn with_source(error: &str, source: Box<dyn Error + Send + Sync>, retriable: bool) -> SRCError {
        SRCError {
            error: error.to_owned(),
            cause: Some(source.to_string()),
            retriable,
            cached: false,
            source: Some(Arc::from(source)),
        }
    }
    pub fn non_retryable_without_cause(error: &str) -> SRCError {
        SRCError::new(error, None, false)
    }
    /// Should be called before putting the error in the cache
    pub fn into_cache(self) -> SRCError {
        SRCError {
            cached: true,
            ..self
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::error::SRCError;
    use std::error::Error;

    #[test]
    fn display_error_no_cause() {
        let err = SRCError::new("Could not get id from response", None, false);
        assert_eq!(format!("{}", err), "Error: Could not get id from response had no other cause, it\'s retriable: false, it\'s cached: false".to_owned())
    }

    #[test]
    fn display_error_with_cause() {
        let err = SRCError::new(
            "Could not get id from response",
            Some(String::from("error in response")),
            false,
        );
        assert_eq!(format!("{}", err), "Error: Could not get id from response, was cause by error in response, it\'s retriable: false, it\'s cached: false".to_owned())
    }

    #[test]
    fn source_is_kept_and_survives_clone_and_cache() {
        let io = std::io::Error::new(std::io::ErrorKind::TimedOut, "timed out");
        let err = SRCError::retryable_with_cause(io, "could not reach the registry").into_cache();
        assert_eq!(err.cause.as_deref(), Some("timed out"));
        let cloned = err.clone();
        let source = cloned.source().expect("source should be kept");
        let io = source.downcast_ref::<std::io::Error>().unwrap();
        assert_eq!(io.kind(), std::io::ErrorKind::TimedOut);
        assert_eq!(err, cloned);
    }

    #[test]
    fn string_cause_and_no_cause() {
        let err = SRCError::non_retryable_with_cause("bad input", "could not parse");
        assert_eq!(err.cause.as_deref(), Some("bad input"));
        assert!(SRCError::non_retryable_without_cause("x")
            .source()
            .is_none());
    }
}
