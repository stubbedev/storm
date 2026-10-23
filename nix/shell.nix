{
  perSystem = {
    pkgs,
    self',
    ...
  }: {
    devShells.default = pkgs.mkShell {
      inputsFrom = [self'.packages.default];

      packages = with pkgs; [
        gopls
        golangci-lint
        delve
        just
        git
        gh
      ];

      env.GOTOOLCHAIN = "local";
    };

    formatter = pkgs.alejandra;
  };
}
