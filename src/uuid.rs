// Just used to build the protobuf encoded form of UUIDs.
include!(concat!(env!("OUT_DIR"), "/uuid.rs"));

pub fn new_v4_proto_uuid() -> self::Uuid {
    let uuid = uuid::Uuid::new_v4();
    let encoded_bytes = uuid.as_bytes();
    self::Uuid {
        value: encoded_bytes.to_vec(),
    }
}

pub fn encode_to_proto(uuid: &uuid::Uuid) -> self::Uuid {
    self::Uuid {
        value: Vec::from(*uuid.as_bytes()),
    }
}

pub fn decode_from_proto(proto_uuid: &Option<self::Uuid>) -> Result<uuid::Uuid, String> {
    proto_uuid
        .as_ref()
        .ok_or_else(|| "No UUID found in proto!".to_string())
        .and_then(|p| {
            uuid::Uuid::from_slice(&p.value)
                .map_err(|err| format!("Could not parse UUID: {:?}", err))
        })
}
