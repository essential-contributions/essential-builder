{
  description = ''
    A nix flake for the essential block builder.
  '';

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixos-unstable";
    systems.url = "github:nix-systems/default";

    # The essential node.
    essential-node = {
      url = "git+ssh://git@github.com/essential-contributions/essential-node";
      inputs.nixpkgs.follows = "nixpkgs";
      inputs.systems.follows = "nixpkgs";
    };
  };

  outputs = inputs:
    let
      overlays = [
        inputs.essential-node.overlays.default
        inputs.self.overlays.default
      ];
      perSystemPkgs = f:
        inputs.nixpkgs.lib.genAttrs (import inputs.systems)
          (system: f (import inputs.nixpkgs { inherit overlays system; }));
    in
    {
      overlays = {
        essential-builder = import ./overlay.nix { };
        default = inputs.self.overlays.essential-builder;
      };

      packages = perSystemPkgs (pkgs: {
        essential-builder = pkgs.essential-builder;
        default = inputs.self.packages.${pkgs.system}.essential-builder;
      });

      devShells = perSystemPkgs (pkgs: {
        essential-builder-dev = pkgs.callPackage ./shell.nix { };
        default = inputs.self.devShells.${pkgs.system}.essential-builder-dev;
      });

      formatter = perSystemPkgs (pkgs: pkgs.nixpkgs-fmt);
    };
}
