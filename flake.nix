{
  description = "Binpocket server";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-22.11";
    naersk = {
      url = "github:nix-community/naersk/master";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    utils = {
      url = "github:numtide/flake-utils";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = { self, nixpkgs, utils, naersk }:
    utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs { inherit system; };
        naersk-lib = pkgs.callPackage naersk { };
      in
        {
          packages.binpocket = naersk-lib.buildPackage {
            src = ./.;
            PROTOC = "${pkgs.protobuf}/bin/protoc";
          };
          nixosModules.binpocket = import ./nix/modules/binpocket.nix {
            binpocket = self.packages.${system}.binpocket;
          };
          defaultPackage = self.packages.${system}.binpocket;
          devShell = with pkgs; mkShell {
            buildInputs = [ cargo rustc rustfmt pre-commit rustPackages.clippy ];
            RUST_SRC_PATH = rustPlatform.rustLibSrc;
          };
      });
}
