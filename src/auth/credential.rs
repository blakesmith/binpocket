use serde::Serialize;

#[derive(Debug)]
pub enum Credential {
    BearerToken(BearerToken),
}

#[derive(Debug)]
pub struct BearerToken {
    pub token: String,
}

impl BearerToken {
    pub fn new(token: String) -> Self {
        Self { token }
    }
}

impl Credential {
    pub fn bearer_from_str(header: &str) -> Option<Credential> {
        match header.split_once("Bearer ") {
            Some((_, token)) => Some(Credential::BearerToken(BearerToken::new(token.to_string()))),
            None => None,
        }
    }
}

#[derive(Serialize, Debug)]
pub struct OAuthAccessTokenResponse {
    token: String,
    access_token: String,
}

impl From<BearerToken> for OAuthAccessTokenResponse {
    fn from(bearer_token: BearerToken) -> OAuthAccessTokenResponse {
        OAuthAccessTokenResponse {
            access_token: bearer_token.token.clone(),
            token: bearer_token.token.clone(),
        }
    }
}
