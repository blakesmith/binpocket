pub mod credential;
pub mod principal;
pub mod resource;

use async_trait::async_trait;
use jwt_simple::algorithms::{ECDSAP256PublicKeyLike, ES256PublicKey};
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use warp::{
    http::{
        header::{HeaderName, HeaderValue},
        StatusCode,
    },
    Filter, Rejection, Reply,
};

use self::credential::{Credential, JWTToken, UsernamePassword};
use self::principal::{Principal, User, UserClaims};
use self::resource::{Action, Resource};

use crate::error::{ErrorCode, ErrorResponse};

#[derive(PartialEq, Debug)]
pub enum Visibility {
    Public,
    Private,
}

pub trait AuthzTarget {
    fn visibility(&self) -> Visibility;
    fn resource(&self) -> Resource;
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
    JWT(jwt_simple::Error),
    InvalidCredentials,
    MissingUser,
}

impl From<jwt_simple::Error> for AuthenticationError {
    fn from(err: jwt_simple::Error) -> AuthenticationError {
        AuthenticationError::JWT(err)
    }
}

impl warp::reject::Reject for AuthenticationError {}

#[async_trait]
/// Main trait that processes credentials, and exchanges them
/// for a Principal.
pub trait Authenticator: Send + Sync {
    async fn authenticate(&self, credential: Credential) -> Result<Principal, AuthenticationError>;
}

/// Authenticator that authenticates with simple in-memory
/// passwords only, in exchange for users.
pub struct FixedPrincipalAuthenticator {
    jwt_public_key: ES256PublicKey,
    user_passwords: HashMap<String, String>,
    users_by_username: HashMap<String, User>,
}

impl FixedPrincipalAuthenticator {
    pub fn new(jwt_public_key: ES256PublicKey) -> Self {
        Self {
            jwt_public_key,
            user_passwords: HashMap::new(),
            users_by_username: HashMap::new(),
        }
    }

    pub fn add_user(&mut self, user: User, password: &str) {
        self.user_passwords
            .insert(user.name.to_string(), password.to_string());
        self.users_by_username.insert(user.name.to_string(), user);
    }
}

#[async_trait]
impl Authenticator for FixedPrincipalAuthenticator {
    async fn authenticate(&self, credential: Credential) -> Result<Principal, AuthenticationError> {
        match credential {
            Credential::UsernamePassword(UsernamePassword {
                ref username,
                ref password,
            }) => self
                .user_passwords
                .get(username)
                .and_then(|pw| {
                    if pw == password {
                        self.users_by_username.get(username)
                    } else {
                        tracing::debug!("Invalid password for username: {}", username);
                        None
                    }
                })
                .ok_or(AuthenticationError::InvalidCredentials)
                .map(|user| Principal::User(user.clone())),
            Credential::JWTToken(JWTToken { ref token }) => {
                let claims = self
                    .jwt_public_key
                    .verify_token::<UserClaims>(token, None)?;
                self.users_by_username
                    .get(&claims.custom.username)
                    .map(|user| Principal::User(user.clone()))
                    .ok_or(AuthenticationError::MissingUser)
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

/// Username / password authentication is only supported on the '/token' endpoint
/// as a way to exchange a username password for a JWT bearer token.
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
    pub fn authorize<T>(
        &self,
        principal: Option<Principal>,
        _action: Action,
        t: T,
    ) -> Result<T, Rejection>
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
    action: Action,
    authorizer: Arc<Authorizer>,
) -> Result<T, Rejection>
where
    T: AuthzTarget,
{
    authorizer.authorize(principal, action, t)
}
