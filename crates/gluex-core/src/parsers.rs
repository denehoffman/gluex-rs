use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, NaiveTime, Utc};

use crate::errors::ParseTimestampError;

/// Parses a timestamp string into a [`DateTime`] in the [`Utc`] timezone, inferring missing fields.
///
/// # Errors
///
/// Returns a [`ParseTimestampError`] if the input cannot be interpreted as a valid timestamp.
pub fn parse_timestamp(input: &str) -> Result<DateTime<Utc>, ParseTimestampError> {
    let digits: Vec<i32> = input
        .split(|c: char| !c.is_ascii_digit())
        .filter(|s| !s.is_empty())
        .filter_map(|s| s.parse::<i32>().ok())
        .collect();
    if digits.is_empty() {
        return Err(ParseTimestampError::NoDigits(input.to_string()));
    }
    let year = digits[0];
    let month = digits.get(1).copied().unwrap_or(12) as u32;
    let day = digits.get(2).copied().unwrap_or_else(|| {
        let start = NaiveDate::from_ymd_opt(year, month, 1)
            .unwrap_or_else(|| NaiveDate::from_ymd_opt(1970, 1, 1).unwrap());
        let next_month = if month == 12 {
            NaiveDate::from_ymd_opt(year + 1, 1, 1)
        } else {
            NaiveDate::from_ymd_opt(year, month + 1, 1)
        }
        .unwrap_or(start);
        next_month.pred_opt().unwrap_or(start).day() as i32
    }) as u32;
    let hour = digits.get(3).copied().unwrap_or(23) as u32;
    let minute = digits.get(4).copied().unwrap_or(59) as u32;
    let second = digits.get(5).copied().unwrap_or(59) as u32;

    let date = NaiveDate::from_ymd_opt(year, month, day).ok_or_else(|| {
        ParseTimestampError::ChronoError(format!("invalid date: {year}-{month}-{day}"))
    })?;
    let time = NaiveTime::from_hms_opt(hour, minute, second).ok_or_else(|| {
        ParseTimestampError::ChronoError(format!("invalid time: {hour}:{minute}:{second}"))
    })?;
    let naive = NaiveDateTime::new(date, time);
    Ok(DateTime::<Utc>::from_naive_utc_and_offset(naive, Utc))
}
