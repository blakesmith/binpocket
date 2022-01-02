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
  cargoSha256 = "12c8rsrm9h7b7l92q7n1wy6v37sbm80j16v06v36nw7yrani0v08";

  # doc tests fail due to missing dependency
  doCheck = false;

  PROTOC = "${pkgs.protobuf}/bin/protoc";

  meta = with lib; {
    description = "Binpocket server";
    homepage = "https://blakesmith.me";
    license = with licenses; [ mit asl20 ];
  };
}
