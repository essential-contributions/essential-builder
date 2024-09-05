# An overlay to make it easier to merge all essential-builder related packages
# into nixpkgs.
{}: final: prev: {
  essential-builder = prev.callPackage ./pkgs/essential-builder.nix { };
}
