use crate::auth::{
    self,
    resource::{Action, Resource},
    AuthzTarget, Visibility,
};
use std::sync::Arc;
use warp::{Filter, Rejection};

#[derive(Debug)]
pub struct Repository {
    pub name: String,
}

impl AuthzTarget for Repository {
    fn visibility(&self) -> Visibility {
        Visibility::Private
    }

    fn resource(&self) -> Resource {
        Resource::Repository
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

pub fn authorize_repository(
    action: Action,
) -> impl Filter<Extract = (Repository,), Error = Rejection> + Clone {
    auth::authenticate()
        .and(repository())
        .and(warp::filters::ext::get::<Arc<auth::Authorizer>>())
        .and_then(move |principal, repo, authorizer| {
            auth::authorize(principal, repo, action, authorizer)
        })
}
