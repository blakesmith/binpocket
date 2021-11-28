{ pkgs ? import (fetchTarball "https://github.com/NixOS/nixpkgs/archive/release-21.05.tar.gz") {} }:

pkgs.mkShell {
  buildInputs = [
    pkgs.flatbuffers
    pkgs.rustup
  ];
}
