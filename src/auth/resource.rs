#[derive(Debug, Clone, Copy)]
pub enum Action {
    Read,
    Write,
}

#[derive(Debug, Clone, Copy)]
pub enum Resource {
    Repository,
}

#[derive(Debug, Clone)]
pub struct Scope {
    pub action: Action,
    pub resource: Resource,
}

impl Scope {
    fn new(action: Action, resource: Resource) -> Self {
        Self { action, resource }
    }
}
