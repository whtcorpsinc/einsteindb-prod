use snafu::Snafu;
use std::{borrow::Cow, ops::RangeInclusive};

/// Length constraints for a database name.
///
/// A `RangeInclusive` is a closed interval, covering [1, 64]
const LENGTH_CONSTRAINT: RangeInclusive<usize> = 1..=64;

/// Database name validation errors.
#[derive(Debug, Snafu)]
pub enum DatabaseNameError {
    #[snafu(display(
        "Database name {} length must be between {} and {} characters",
        name,
        LENGTH_CONSTRAINT.start(),
        LENGTH_CONSTRAINT.end()
    ))]
    LengthConstraint { name: String },

    #[snafu(display(
        "Database name '{}' contains invalid character. Character number {} is a control which is not allowed.", name, bad_char_offset
    ))]
    BadChars {
        bad_char_offset: usize,
        name: String,
    },
}

//It has a constructor that takes a string and converts it into a Cow<'a, str>
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DatabaseName<'a>(Cow<'a, str>);

impl<'a> DatabaseName<'a> {
    //2. It has a new function that takes a string and converts it into a Cow<'a, str>
    pub fn new<T: Into<Cow<'a, str>>>(name: T) -> Result<Self, DatabaseNameError> {
        // Cow is a type that is used to represent a borrow of a string.
        //It is a smart pointer that can either be a reference or a copy.
        let name: Cow<'a, str> = name.into();

        if !LENGTH_CONSTRAINT.contains(&name.len()) {
            return Err(DatabaseNameError::LengthConstraint {
                name: name.to_string(),
            });
        }