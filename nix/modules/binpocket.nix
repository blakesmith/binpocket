{ config, lib, pkgs, ... }:
with lib;
let
  cfg = config.services.binpocket;
  configAttributes = {
    data_path = "/var/lib/${cfg.dataDir}";
    jwt_issuer = cfg.jwtIssuer;
    web_root = cfg.webRoot;
    listen_port = cfg.listenPort;
    users = cfg.users;
  };
  configFile = pkgs.writeTextFile {
    name = "binpocket.yaml";
    text = lib.generators.toYAML {} configAttributes;
  };
in {
  options = {
    services.binpocket = {
      enable = mkEnableOption "binpocket server";
      dataDir = mkOption {
        type = types.str;
        default = "binpocket";
        example = "binpocket";
        description = "Data state dir name";
      };
      jwtIssuer = mkOption {
        type = types.str;
        default = "binpocket";
        example = "binpocket";
        description = "The name of the JWT issuer in token payloads.";
      };
      webRoot = mkOption {
        type = types.str;
        example = "http://127.0.0.1:3030";
        description = "Web root URL.";
      };
      listenPort = mkOption {
        type = types.int;
        example = 3030;
        default = 3030;
        description = "Service port to listen on";
      };
      users = mkOption {
        type = types.listOf types.attrs;
        default = [];
        example = [
          { username = "my_username"; password = "my_password"; global_scopes = [{action = "Read"; resource = "Repository";}]; }
        ];
      };
    };
  };

  #### Implementation

  config = mkIf cfg.enable {
    users.users.binpocket = {
      createHome = true;
      description = "binpocket user";
      isSystemUser = true;
      group = "binpocket";
      home = "/srv/binpocket";
    };

    users.groups.binpocket.gid = 1000;

    systemd.services.binpocket = {
      description = "binpocket server";
      after = [ "network.target" ];
      wantedBy = [ "multi-user.target" ];
      script = ''
      exec ${pkgs.binpocket}/bin/binpocket \
      --config ${configFile}
      '';

      serviceConfig = {
        Type = "simple";
        User = "binpocket";
        Group = "binpocket";
        Restart = "on-failure";
        RestartSec = "30s";
        StateDirectory = "${cfg.dataDir}";
      };
    };
  };
}
