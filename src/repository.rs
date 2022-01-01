use crate::auth::{
    self,
    resource::{Action, Resource},
    AuthzTarget, Visibility,
};
use crate::manifest::ManifestStore;
use crate::ulid as ulid_util;
use std::sync::Arc;
use ulid::Ulid;
use warp::{Filter, Rejection};

pub mod protos {
    include!(concat!(env!("OUT_DIR"), "/binpocket.repository.rs"));
}

#[derive(Debug)]
pub struct Repository {
    pub id: Ulid,
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

async fn lookup_repository<M: ManifestStore + Send + Sync + 'static>(
    name: String,
    manifest_store: Arc<M>,
) -> Result<Repository, Rejection> {
    tracing::debug!("Repository name is: {}", name);
    let repository = manifest_store.get_or_create_repository(&name).await?;
    let id = ulid_util::decode_from_proto(&repository.id);
    Ok(Repository {
        id: id,
        name: repository.name,
    })
}

pub fn repository<M: ManifestStore + Send + Sync + 'static>(
) -> impl Filter<Extract = (Repository,), Error = Rejection> + Clone {
    warp::path::param()
        .and(warp::filters::ext::get::<Arc<M>>())
        .and_then(lookup_repository)
        .boxed()
}

pub fn authorize_repository<M: ManifestStore + Send + Sync + 'static>(
    action: Action,
) -> impl Filter<Extract = (Repository,), Error = Rejection> + Clone {
    auth::authenticate()
        .and(repository::<M>())
        .and(warp::filters::ext::get::<Arc<auth::Authorizer>>())
        .and_then(move |principal, repo, authorizer| {
            auth::authorize(principal, repo, action, authorizer)
        })
}
