pub mod repository {
    include!(concat!(env!("OUT_DIR"), "/binpocket.repository.rs"));
}

pub mod manifest {
    use serde::Deserialize;

    include!(concat!(env!("OUT_DIR"), "/binpocket.manifest.rs"));
}

pub mod ulid {
    include!(concat!(env!("OUT_DIR"), "/ulid.rs"));

    pub fn new_proto() -> self::Ulid {
        encode_to_proto(ulid::Ulid::new())
    }

    pub fn ulid_bytes(ulid: &ulid::Ulid) -> [u8; 16] {
        ulid.0.to_be_bytes()
    }

    pub fn encode_to_proto(ulid: ulid::Ulid) -> self::Ulid {
        let parts: (u64, u64) = ulid.into();
        self::Ulid {
            msb: parts.0,
            lsb: parts.1,
        }
    }

    pub fn decode_from_proto(proto_ulid: &Option<self::Ulid>) -> ulid::Ulid {
        match proto_ulid {
            Some(proto) => ulid::Ulid::from((proto.msb, proto.lsb)),
            None => ulid::Ulid::default(),
        }
    }
}
