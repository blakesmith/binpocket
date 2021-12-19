use serde::Deserialize;

#[derive(Deserialize, Debug, Clone, Copy, PartialEq)]
pub enum Action {
    Read,
    Write,
}

#[derive(Deserialize, Debug, Clone, Copy, PartialEq)]
pub enum Resource {
    Repository,
}

#[derive(Deserialize, Debug, Clone, PartialEq)]
pub struct Scope {
    pub action: Action,
    pub resource: Resource,
}

#[test]
fn test_deserializing_scopes() {
    let serialized_scope = "{\"action\":\"Read\",\"resource\":\"Repository\"}";
    let scope: Scope = serde_json::from_str(serialized_scope).expect("Could not deserialize scope");
    assert_eq!(Action::Read, scope.action);
    assert_eq!(Resource::Repository, scope.resource);
}
