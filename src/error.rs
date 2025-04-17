//! implementation for [`SRCError`]
use std::error::Error;
use std::fmt;
use std::fmt::Display;

/// Error struct which makes it easy to know if the resulting error is also preserved in the cache
/// or not. And whether trying it again might not cause an error.
#[derive(Debug, PartialEq)]
pub struct SRCError {
    pub error: String,
    pub cause: Option<String>,
    pub retriable: bool,
    pub cached: bool,
}

/// Implements standard error so error handling can be simplified
impl Error for SRCError {}

/// Implements clone so when an error is returned from the cache, a copy can be returned
impl Clone for SRCError {
    fn clone(&self) -> SRCError {
        SRCError {
            error: self.error.clone(),
            cause: self.cause.as_ref().cloned(),
            retriable: self.retriable,
            cached: self.cached,
        }
    }
}

/// Gives the information from the error in a readable format.
impl fmt::Display for SRCError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self.cause {
            Some(cause) => write!(
                f,
                "Error: {}, was cause by {}, it's retriable: {}, it's cached: {}",
                self.error, &cause, self.retriable, self.cached
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
        }
    }
    pub fn retryable_with_cause<T: Display>(cause: T, error: &str) -> SRCError {
        SRCError::new(error, Some(format!("{}", cause)), true)
    }
    pub fn non_retryable_with_cause<T: Display>(cause: T, error: &str) -> SRCError {
        SRCError::new(error, Some(format!("{}", cause)), false)
    }
    pub fn non_retryable_without_cause(error: &str) -> SRCError {
        SRCError::new(error, None, false)
    }
    /// Should be called before putting the error in the cache
    pub fn into_cache(self) -> SRCError {
        SRCError {
            error: self.error,
            cause: self.cause,
            retriable: self.retriable,
            cached: true,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::error::SRCError;

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
}
