use std::io::Result;

fn main() -> Result<()> {
    let mut config = prost_build::Config::new();
    config.type_attribute(
        ".binpocket.manifest.ImageManifestV2",
        "#[derive(Deserialize)]",
    );
    config.type_attribute(
        ".binpocket.manifest.ImageManifestV2",
        "#[serde(rename_all = \"camelCase\")]",
    );

    config.type_attribute(".binpocket.manifest.MediaV2", "#[derive(Deserialize)]");
    config.type_attribute(
        ".binpocket.manifest.MediaV2",
        "#[serde(rename_all = \"camelCase\")]",
    );

    config.compile_protos(
        &[
            "protos/manifest.proto",
            "protos/repository.proto",
            "protos/uuid.proto",
        ],
        &["protos/"],
    )?;
    Ok(())
}
