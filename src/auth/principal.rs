use serde::{Deserialize, Serialize};

/// These are used as JWT claims, and are
/// serialized / deserialized inside an
/// authenticated JWT token.
#[derive(Debug, Serialize, Deserialize)]
pub struct UserClaims<'user> {
    pub username: &'user str,
}

#[derive(Debug, Clone)]
pub struct User {
    pub name: String,
}

impl<'user> From<&'user User> for UserClaims<'user> {
    fn from(user: &User) -> UserClaims {
        UserClaims {
            username: &user.name,
        }
    }
}

#[derive(Debug, Clone)]
pub enum Principal {
    User(User),
}
