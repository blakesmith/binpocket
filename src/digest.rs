use std::convert::TryFrom;

/// Parse digests in the form of 'algorithm:encoded' into a real
/// Digest type.
pub fn deserialize_digest_string<'de, D>(deserializer: D) -> Result<Digest, D::Error>
where
    D: serde::de::Deserializer<'de>,
{
    let value: &str = serde::de::Deserialize::deserialize(deserializer)?;
    Digest::try_from(value).map_err(serde::de::Error::custom)
}

#[derive(Debug, PartialEq)]
pub enum DigestAlgorithm {
    Sha256,
    Sha512,
    Other(String),
}

impl std::fmt::Display for DigestAlgorithm {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let s = match self {
            DigestAlgorithm::Sha256 => "sha256",
            DigestAlgorithm::Sha512 => "sha512",
            DigestAlgorithm::Other(o) => o,
        };

        write!(f, "{}", s)
    }
}

/// Content identifier for Blobs, as well as Manifest objects inside
/// repositories.
#[derive(Debug, PartialEq)]
pub struct Digest {
    algorithm: DigestAlgorithm,
    encoded: String,
}

impl Digest {
    fn new(algorithm: DigestAlgorithm, encoded: String) -> Self {
        Digest { algorithm, encoded }
    }
}

impl TryFrom<&str> for Digest {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        // TODO: Correctly handle the regexp limitations from
        // the digest specification: https://github.com/opencontainers/image-spec/blob/v1.0.1/descriptor.md#digests
        let parts: Vec<&str> = value.split(':').collect();
        if parts.len() != 2 {
            return Err(format!(
                "Invalid digest: expected 2 parts, got {}",
                parts.len()
            ));
        }
        let algorithm = match parts[0] {
            "sha256" => DigestAlgorithm::Sha256,
            "sha512" => DigestAlgorithm::Sha512,
            other => DigestAlgorithm::Other(other.to_string()),
        };

        Ok(Digest {
            algorithm,
            encoded: parts[1].to_string(),
        })
    }
}

impl std::fmt::Display for Digest {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}:{}", self.algorithm, self.encoded)
    }
}

#[test]
fn test_serialize_deserialize_digests() {
    let value = "sha256:6c3c624b58dbbcd3c0dd82b4c53f04194d1247c6eebdaab7c610cf7d66709b3b";
    let parsed = Digest::try_from(value).unwrap();

    assert_eq!(
        parsed,
        Digest::new(
            DigestAlgorithm::Sha256,
            "6c3c624b58dbbcd3c0dd82b4c53f04194d1247c6eebdaab7c610cf7d66709b3b".to_string()
        )
    );

    assert_eq!(value, format!("{}", parsed));
}
