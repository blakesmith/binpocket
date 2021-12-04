#[derive(Debug, Clone)]
pub struct User {
    pub name: String,
}

#[derive(Debug, Clone)]
pub enum Principal {
    User(User),
}
