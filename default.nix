{ pkgs ? import (fetchTarball "https://github.com/NixOS/nixpkgs/archive/release-21.11.tar.gz") {},
  lib ? pkgs.lib,
  rustPlatform ? pkgs.rustPlatform,
}:
with rustPlatform;

buildRustPackage rec {
  pname = "binpocket";
  version = "0.1.0";

  src = builtins.filterSource
    (path: type: type != "directory" || baseNameOf path != "target")
    ./.;
  cargoSha256 = "1yqkxq0076qq9d4am6mznsprv1qz71jlm0pdpcsqn5r9hyc2d421";

  # doc tests fail due to missing dependency
  doCheck = false;

  PROTOC = "${pkgs.protobuf}/bin/protoc";

  meta = with lib; {
    description = "Binpocket server";
    homepage = "https://blakesmith.me";
    license = with licenses; [ mit asl20 ];
  };
}
