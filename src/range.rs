use serde::Deserialize;
use std::convert::TryFrom;

#[derive(Debug, PartialEq, Deserialize)]
pub struct ContentRange {
    start: u64,
    end: u64,
}

impl TryFrom<&str> for ContentRange {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let parts: Vec<&str> = value.split('-').collect();
        if parts.len() != 2 {
            return Err(format!(
                "Invalid Content-Range. Expected 2 parts, got {}. Value: {}",
                parts.len(),
                value
            ));
        }
        let start = parts[0]
            .parse::<u64>()
            .map_err(|e| format!("Failed to parse start: {:?}", e))?;
        let end = parts[1]
            .parse::<u64>()
            .map_err(|e| format!("Failed to parse start: {:?}", e))?;
        Ok(Self { start, end })
    }
}

#[test]
fn test_parses_correct_content_range() {
    assert_eq!(
        Ok(ContentRange { start: 5, end: 10 }),
        TryFrom::try_from("5-10")
    );
}

#[test]
fn test_incorrect_content_range() {
    let content_range: Result<ContentRange, String> = TryFrom::try_from("5-");
    assert_eq!(true, content_range.is_err());
}
