pub mod principal;

use async_trait::async_trait;
use std::sync::Arc;
use warp::{Filter, Rejection};

use self::principal::{Principal, User};

pub enum Credential {
    BearerToken(String),
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
    token: String,
    principal: Principal,
}

#[async_trait]
impl Authenticator for FixedBearerTokenAuthenticator {
    async fn authenticate(&self, credential: Credential) -> Result<Principal, AuthenticationError> {
        match credential {
            Credential::BearerToken(token) => {
                if token == self.token {
                    Ok(self.principal.clone())
                } else {
                    tracing::debug!("Bearer token credentials don't match");
                    Err(AuthenticationError::InvalidCredentials)
                }
            }
            _ => Err(AuthenticationError::UnknownCredentialType),
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
