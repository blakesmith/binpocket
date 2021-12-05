use crate::auth::{AuthzTarget, Visibility};
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
