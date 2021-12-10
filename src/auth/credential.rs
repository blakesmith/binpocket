use serde::Serialize;
use std::sync::Arc;
use warp::{
    http::{Response, StatusCode},
    Filter, Rejection, Reply,
};

use crate::error::{ErrorCode, ErrorResponse};

use super::authenticate_basic;
use super::principal::Principal;

#[derive(Debug, PartialEq)]
pub struct BearerToken {
    pub token: String,
}

impl BearerToken {
    pub fn new(token: String) -> Self {
        Self { token }
    }
}

#[derive(Debug, PartialEq)]
pub struct UsernamePassword {
    pub username: String,
    pub password: String,
}

impl UsernamePassword {
    pub fn new(username: String, password: String) -> Self {
        Self { username, password }
    }
}

#[derive(Debug, PartialEq)]
pub enum Credential {
    BearerToken(BearerToken),
    UsernamePassword(UsernamePassword),
}

impl Credential {
    /// Extract a bearer token from a header value.
    pub fn bearer_from_header(header: &str) -> Option<Credential> {
        match header.split_once("Bearer ") {
            Some((_, token)) => Some(Credential::BearerToken(BearerToken::new(token.to_string()))),
            None => None,
        }
    }

    pub fn basic_from_header(header: &str) -> Option<Credential> {
        header.split_once("Basic ").and_then(|(_, base64_creds)| {
            base64::decode(base64_creds)
                .ok()
                .and_then(|decoded| std::str::from_utf8(&decoded).ok().map(|d| d.to_string()))
                .and_then(|plaintext_creds| {
                    plaintext_creds.split_once(":").map(|(username, password)| {
                        Credential::UsernamePassword(UsernamePassword::new(
                            username.to_string(),
                            password.to_string(),
                        ))
                    })
                })
        })
    }
}

/// The access token response that's returned upon successfull
/// username / password authentication. We return a 'token' value
/// that's consumed by the docker agent. The 'access_token' value
/// is also present for compatibility, and must always match the
/// token value.
#[derive(Serialize, Debug)]
struct OAuthAccessTokenResponse {
    token: String,
    access_token: String,
}

impl From<BearerToken> for OAuthAccessTokenResponse {
    fn from(bearer_token: BearerToken) -> OAuthAccessTokenResponse {
        OAuthAccessTokenResponse {
            access_token: bearer_token.token.clone(),
            token: bearer_token.token.clone(),
        }
    }
}

pub(crate) struct JWTTokenGenerator {}

impl JWTTokenGenerator {
    pub fn generate_bearer_token(&self, _principal: &Principal) -> BearerToken {
        BearerToken::new("a_global_test_token".to_string())
    }
}

async fn access_token_response(
    principal: Option<Principal>,
    jwt_generator: Arc<JWTTokenGenerator>,
) -> Result<Response<Vec<u8>>, Rejection> {
    match principal {
        Some(p) => {
            let token = jwt_generator.generate_bearer_token(&p);
            let oauth_response: OAuthAccessTokenResponse = token.into();
            Ok(warp::http::response::Builder::new()
                .status(StatusCode::OK)
                .body(serde_json::to_vec(&oauth_response).unwrap())
                .unwrap())
        }
        None => Err(ErrorResponse::new(
            StatusCode::UNAUTHORIZED,
            ErrorCode::Denied,
            "Access denied".to_string(),
        )
        .into()),
    }
}

pub(crate) fn oauth_access_token() -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    warp::get()
        .and(warp::path!("token"))
        .and(authenticate_basic())
        .and(warp::filters::ext::get::<Arc<JWTTokenGenerator>>())
        .and_then(access_token_response)
        .boxed()
}

#[test]
fn test_credential_from_basic() {
    let credential = Credential::basic_from_header("Basic Zml4ZWQ6YV9nbG9iYWxfdGVzdF90b2tlbg==");
    assert_eq!(
        credential,
        Some(Credential::UsernamePassword(UsernamePassword::new(
            "fixed".to_string(),
            "a_global_test_token".to_string(),
        )))
    );
}
