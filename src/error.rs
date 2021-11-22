use serde::Serialize;
use std::convert::Infallible;
use warp::{http::status::StatusCode, Rejection, Reply};

#[derive(Debug, Serialize)]
pub enum ErrorCode {
    #[serde(rename = "DIGEST_INVALID")]
    DigestInvalid,
}

#[derive(Debug, Serialize)]
pub struct ErrorMessage {
    code: ErrorCode,
    message: String,
    detail: String,
}

#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    #[serde(skip_serializing)]
    http_code: StatusCode,
    errors: Vec<ErrorMessage>,
}

#[derive(Serialize)]
pub struct SimpleError {
    message: String,
}

impl warp::reject::Reject for ErrorResponse {}

pub async fn handle_rejection(err: Rejection) -> Result<impl Reply, Infallible> {
    if err.is_not_found() {
        Ok(warp::reply::with_status(
            warp::reply::json(&SimpleError {
                message: "Not found".to_string(),
            }),
            StatusCode::NOT_FOUND,
        ))
    } else if let Some(_) = err.find::<warp::reject::MethodNotAllowed>() {
        Ok(warp::reply::with_status(
            warp::reply::json(&SimpleError {
                message: "Method not allowed".to_string(),
            }),
            StatusCode::METHOD_NOT_ALLOWED,
        ))
    } else if let Some(err_resp) = err.find::<ErrorResponse>() {
        Ok(warp::reply::with_status(
            warp::reply::json(err_resp),
            err_resp.http_code,
        ))
    } else {
        tracing::error!("Got unhandled error: {:?}", err);
        Ok(warp::reply::with_status(
            warp::reply::json(&SimpleError {
                message: "Something broke".to_string(),
            }),
            StatusCode::INTERNAL_SERVER_ERROR,
        ))
    }
}
