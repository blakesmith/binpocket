use warp::{Filter, Rejection};

#[derive(Debug)]
pub struct Repository {
    pub name: String,
}

pub fn repository() -> impl Filter<Extract = (Repository,), Error = Rejection> + Copy {
    warp::path::param().map(|name| {
        tracing::debug!("Repository name is: {}", name);
        Repository { name }
    })
}
