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
  cargoSha256 = "0iq1y4i13s3ldfwny350yiwxind8bv46s1ij9lvjqahjhpkl4zmg";

  # doc tests fail due to missing dependency
  doCheck = false;

  PROTOC = "${pkgs.protobuf}/bin/protoc";

  meta = with lib; {
    description = "Binpocket server";
    homepage = "https://blakesmith.me";
    license = with licenses; [ mit asl20 ];
  };
}
