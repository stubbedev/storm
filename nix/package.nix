{self, ...}: {
  perSystem = {
    lib,
    pkgs,
    ...
  }: {
    packages.default = pkgs.buildGo127Module {
      pname = "ormgen";
      version = "0-unstable-${self.lastModifiedDate or "0"}";

      src = lib.fileset.toSource {
        root = ../.;
        fileset = lib.fileset.difference (lib.fileset.gitTracked ../.) (
          lib.fileset.unions [
            ../flake.nix
            ../flake.lock
            ../nix
          ]
        );
      };

      vendorHash = "sha256-S5Fw0YwpXXEP7Fta23urG8Rnp8t/AZTdWiPpi0g/xVU=";

      subPackages = ["cmd/ormgen"];

      env.CGO_ENABLED = 0;

      doCheck = true;

      meta = {
        description = "Code generator for storm models";
        homepage = "https://github.com/stubbedev/storm";
        license = lib.licenses.mit;
        mainProgram = "ormgen";
        platforms = lib.platforms.unix;
      };
    };
  };
}
