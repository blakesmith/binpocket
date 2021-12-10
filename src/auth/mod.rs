pub mod credential;
pub mod principal;

use async_trait::async_trait;
use std::str::FromStr;
use std::sync::Arc;
use warp::{
    http::{
        header::{HeaderName, HeaderValue},
        StatusCode,
    },
    Filter, Rejection, Reply,
};

use self::credential::{BearerToken, Credential, UsernamePassword};
use self::principal::Principal;

use crate::error::{ErrorCode, ErrorResponse};

#[derive(PartialEq, Debug)]
pub enum Visibility {
    Public,
    Private,
}

pub trait AuthzTarget {
    fn visibility(&self) -> Visibility;
}

fn authorization_bearer_header(
) -> impl Filter<Extract = (Option<Credential>,), Error = Rejection> + Clone {
    warp::header::optional::<String>("Authorization").map(|auth_header: Option<String>| {
        auth_header.and_then(|h| Credential::bearer_from_header(&h))
    })
}

fn authorization_basic_header(
) -> impl Filter<Extract = (Option<Credential>,), Error = Rejection> + Clone {
    warp::header::optional::<String>("Authorization").map(|auth_header: Option<String>| {
        auth_header.and_then(|h| Credential::basic_from_header(&h))
    })
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
            Credential::UsernamePassword(UsernamePassword {
                ref username,
                ref password,
            }) => {
                // TODO: Hack: Just match tokens directly for now
                // swap this out for real username / password verification
                if *password == self.token {
                    tracing::debug!("Username / password credentials match. Authenticated!");
                    Ok(self.principal.clone())
                } else {
                    tracing::debug!("Invalid username / password credentials");
                    Err(AuthenticationError::InvalidCredentials)
                }
            }
            Credential::BearerToken(BearerToken { ref token }) => {
                // TODO: Replace with JWT token verification
                if *token == self.token {
                    tracing::debug!("Tokens match. Logged in!");
                    Ok(self.principal.clone())
                } else {
                    tracing::debug!("Bearer token credentials don't match: {:?}", credential);
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

/// By default: Almost all endpoints must authenticate with a Bearer
/// token.
pub fn authenticate() -> impl Filter<Extract = (Option<Principal>,), Error = Rejection> + Clone {
    warp::filters::ext::get::<Arc<Box<dyn Authenticator>>>()
        .and(authorization_bearer_header())
        .and_then(check_authentication)
}

pub fn authenticate_basic() -> impl Filter<Extract = (Option<Principal>,), Error = Rejection> + Clone
{
    warp::filters::ext::get::<Arc<Box<dyn Authenticator>>>()
        .and(authorization_basic_header())
        .and_then(check_authentication)
}

pub fn routes() -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    credential::oauth_access_token()
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
