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
  cargoSha256 = "036jhqy0j1l2dhzvgyln8kh3c809xj689bp66hi48ql1gapjv5i0";

  # doc tests fail due to missing dependency
  doCheck = false;

  PROTOC = "${pkgs.protobuf}/bin/protoc";

  meta = with lib; {
    description = "Binpocket server";
    homepage = "https://blakesmith.me";
    license = with licenses; [ mit asl20 ];
  };
}
