use serde::Deserialize;

use crate::digest::{deserialize_digest_string, Digest};

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
struct Media {
    media_type: String,
    size: u64,

    #[serde(deserialize_with = "deserialize_digest_string")]
    digest: Digest,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
struct ImageManifest {
    schema_version: u8,
    config: Media,
    layers: Vec<Media>,
}
