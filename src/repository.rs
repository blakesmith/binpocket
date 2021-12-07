use crate::auth::{self, AuthzTarget, Visibility};
use std::sync::Arc;
use warp::{Filter, Rejection};

#[derive(Debug)]
pub struct Repository {
    pub name: String,
}

impl AuthzTarget for Repository {
    fn visibility(&self) -> Visibility {
        Visibility::Public
    }
}

pub fn repository() -> impl Filter<Extract = (Repository,), Error = Rejection> + Clone {
    warp::path::param()
        .map(|name| {
            tracing::debug!("Repository name is: {}", name);
            Repository { name }
        })
        .boxed()
}

pub fn authorize_repository() -> impl Filter<Extract = (Repository,), Error = Rejection> + Clone {
    auth::authenticate()
        .and(repository())
        .and(warp::filters::ext::get::<Arc<auth::Authorizer>>())
        .and_then(auth::authorize)
}
