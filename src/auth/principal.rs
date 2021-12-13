use serde::{Deserialize, Serialize};

/// These are used as JWT claims, and are
/// serialized / deserialized inside an
/// authenticated JWT token.
#[derive(Debug, Serialize, Deserialize)]
pub struct UserClaims {
    pub username: String,
}

#[derive(Debug, Clone)]
pub struct User {
    pub name: String,
}

impl From<&User> for UserClaims {
    fn from(user: &User) -> UserClaims {
        UserClaims {
            username: user.name.clone(),
        }
    }
}

#[derive(Debug, Clone)]
pub enum Principal {
    User(User),
}
