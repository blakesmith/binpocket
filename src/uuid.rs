// Just used to build the protobuf encoded form of UUIDs.
include!(concat!(env!("OUT_DIR"), "/uuid.rs"));

pub fn new_v4_proto_uuid() -> self::Uuid {
    let uuid = uuid::Uuid::new_v4();
    let encoded_bytes = uuid.as_bytes();
    self::Uuid {
        value: encoded_bytes.to_vec(),
    }
}
