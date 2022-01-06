use serde::Serialize;
use std::convert::Infallible;
use warp::{
    http::{
        header::{HeaderName, HeaderValue},
        status::StatusCode,
    },
    Rejection, Reply,
};

#[derive(Debug, Serialize)]
pub enum ErrorCode {
    #[serde(rename = "DIGEST_INVALID")]
    DigestInvalid,

    #[serde(rename = "MANIFEST_UNKNOWN")]
    ManifestUnknown,

    #[serde(rename = "MANIFEST_INVALID")]
    ManifestInvalid,

    #[serde(rename = "NAME_UKNOWN")]
    NameUnknown,

    #[serde(rename = "DENIED")]
    Denied,

    #[serde(rename = "UNKNOWN")]
    Unknown,
}

#[derive(Debug, Serialize)]
pub struct ErrorMessage {
    code: ErrorCode,
    message: String,
    detail: String,
}

/// This is an encapsulation of the Error Codes structure
/// for all 4xx level responses in the OCI distribution spec
/// See: https://github.com/opencontainers/distribution-spec/blob/main/spec.md#error-codes
/// for the structure of error codes, and the full list.
#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    #[serde(skip_serializing)]
    http_code: StatusCode,
    errors: Vec<ErrorMessage>,

    #[serde(skip_serializing)]
    headers: Vec<(HeaderName, HeaderValue)>,
}

impl ErrorResponse {
    pub fn new(status: StatusCode, code: ErrorCode, message: String) -> Self {
        ErrorResponse {
            http_code: status,
            errors: vec![ErrorMessage {
                code,
                message,
                detail: "".to_string(),
            }],
            headers: vec![],
        }
    }

    pub fn add_header(&mut self, name: HeaderName, value: HeaderValue) {
        self.headers.push((name, value));
    }
}

#[derive(Serialize)]
pub struct SimpleError {
    message: String,
}

impl warp::reject::Reject for ErrorResponse {}

pub async fn handle_rejection(err: Rejection) -> Result<impl Reply, Infallible> {
    if err.is_not_found() {
        Ok(warp::http::response::Builder::new()
            .status(StatusCode::NOT_FOUND)
            .body(
                serde_json::to_vec(&SimpleError {
                    message: "Not found".to_string(),
                })
                .map_err(|err| tracing::error!("JSON failed: {}", err))
                .unwrap_or_else(|_| vec![]),
            )
            .unwrap())
    } else if let Some(err_resp) = err.find::<ErrorResponse>() {
        let mut builder = warp::http::response::Builder::new().status(err_resp.http_code);
        for (name, value) in &err_resp.headers {
            builder = builder.header(name.clone(), value.clone());
        }
        Ok(builder
            .body(
                serde_json::to_vec(err_resp)
                    .map_err(|err| tracing::error!("JSON failed: {}", err))
                    .unwrap_or_else(|_| vec![]),
            )
            .unwrap())
    } else if let Some(err) = err.find::<warp::reject::MethodNotAllowed>() {
        tracing::debug!("Method not allowed: {:?}", err);
        Ok(warp::http::response::Builder::new()
            .status(StatusCode::METHOD_NOT_ALLOWED)
            .body(
                serde_json::to_vec(&SimpleError {
                    message: "Method not allowed".to_string(),
                })
                .map_err(|err| tracing::error!("JSON failed: {}", err))
                .unwrap_or_else(|_| vec![]),
            )
            .unwrap())
    } else {
        tracing::error!("Got unhandled error: {:?}", err);
        Ok(warp::http::response::Builder::new()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body(
                serde_json::to_vec(&SimpleError {
                    message: "Something broke".to_string(),
                })
                .map_err(|err| tracing::error!("JSON failed: {}", err))
                .unwrap_or_else(|_| vec![]),
            )
            .unwrap())
    }
}
