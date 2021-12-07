pub mod principal;

use async_trait::async_trait;
use std::str::FromStr;
use std::sync::Arc;
use warp::{
    http::{
        header::{HeaderName, HeaderValue},
        StatusCode,
    },
    Filter, Rejection,
};

use self::principal::Principal;

use crate::error::{ErrorCode, ErrorResponse};

pub enum Credential {
    BearerToken(String),
}

#[derive(PartialEq, Debug)]
pub enum Visibility {
    Public,
    Private,
}

pub trait AuthzTarget {
    fn visibility(&self) -> Visibility;
}

fn authorization_header() -> impl Filter<Extract = (Option<Credential>,), Error = Rejection> + Clone
{
    warp::header::optional::<String>("Authorization")
        .map(|auth_token: Option<String>| auth_token.map(Credential::BearerToken))
}

#[derive(Debug)]
pub enum AuthenticationError {
    InvalidCredentials,
    UnknownCredentialType,
}

impl warp::reject::Reject for AuthenticationError {}

#[async_trait]
/// Main trait that processes credentials, and exchanges them
/// for a Principal.
pub trait Authenticator: Send + Sync {
    async fn authenticate(&self, credential: Credential) -> Result<Principal, AuthenticationError>;
}

/// Authenticator that authenticates on a global bearer token, in exchange
/// for a given Principal.
pub struct FixedBearerTokenAuthenticator {
    pub token: String,
    pub principal: Principal,
}

#[async_trait]
impl Authenticator for FixedBearerTokenAuthenticator {
    async fn authenticate(&self, credential: Credential) -> Result<Principal, AuthenticationError> {
        match credential {
            Credential::BearerToken(token) => {
                if token == self.token {
                    tracing::debug!("Tokens match. Logged in!");
                    Ok(self.principal.clone())
                } else {
                    tracing::debug!("Bearer token credentials don't match");
                    Err(AuthenticationError::InvalidCredentials)
                }
            }
        }
    }
}

async fn check_authentication(
    authenticator: Arc<Box<dyn Authenticator>>,
    credential: Option<Credential>,
) -> Result<Option<Principal>, Rejection> {
    match credential {
        Some(c) => match authenticator.authenticate(c).await {
            Ok(principal) => Ok(Some(principal)),
            Err(err) => {
                tracing::debug!("Could not authenticate: {:?}", err);
                Ok(None)
            }
        },
        None => Ok(None),
    }
}

pub fn authenticate() -> impl Filter<Extract = (Option<Principal>,), Error = Rejection> + Clone {
    warp::filters::ext::get::<Arc<Box<dyn Authenticator>>>()
        .and(authorization_header())
        .and_then(check_authentication)
}

#[derive(Debug)]
pub enum AuthorizationError {}

impl warp::reject::Reject for AuthorizationError {}

pub struct Authorizer {
    pub auth_url: String,
}

impl Authorizer {
    pub fn authorize<T>(&self, principal: Option<Principal>, t: T) -> Result<T, Rejection>
    where
        T: AuthzTarget,
    {
        if principal.is_some() || t.visibility() == Visibility::Public {
            Ok(t)
        } else {
            let mut error = ErrorResponse::new(
                StatusCode::UNAUTHORIZED,
                ErrorCode::Denied,
                "Access denied".to_string(),
            );
            error.add_header(
                HeaderName::from_str(&"WWW-Authenticate").unwrap(),
                HeaderValue::from_str(&format!("Bearer realm=\"{}\"", self.auth_url)).unwrap(),
            );
            Err(error.into())
        }
    }
}

pub async fn authorize<T>(
    principal: Option<Principal>,
    t: T,
    authorizer: Arc<Authorizer>,
) -> Result<T, Rejection>
where
    T: AuthzTarget,
{
    authorizer.authorize(principal, t)
}
