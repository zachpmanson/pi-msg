{
  description = "Bridge the Pi coding agent to XMPP: drive Pi entirely from a chat client.";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-26.05";

  outputs =
    { self, nixpkgs }:
    let
      systems = [
        "aarch64-darwin"
        "x86_64-darwin"
        "x86_64-linux"
        "aarch64-linux"
      ];
      forAllSystems = f: nixpkgs.lib.genAttrs systems (system: f (import nixpkgs { inherit system; }));
    in
    {
      packages = forAllSystems (pkgs: rec {
        default = pi-msg;
        pi-msg = pkgs.buildGoModule {
          pname = "pi-msg";
          version = "0.3.0";
          src = ./.;
          # Hash of the Go module dependencies. Bump when go.mod/go.sum change:
          # set to pkgs.lib.fakeHash, run `nix build`, and copy the reported hash.
          vendorHash = "sha256-/yBOILFXGNEgImJz5K4+nHguqaAGlg1UNBYnHiedizM=";
          # Single static bin from package main at the module root.
          meta = {
            description = "Bridge the Pi coding agent to XMPP.";
            homepage = "https://github.com/zachpmanson/pi-msg";
            mainProgram = "pi-msg";
            license = pkgs.lib.licenses.mit;
          };
        };
      });

      devShells = forAllSystems (pkgs: {
        default = pkgs.mkShell {
          packages = [
            pkgs.go
            pkgs.gopls
          ];
          shellHook = ''
            echo "pi-msg dev shell — $(go version)"
          '';
        };
      });

      formatter = forAllSystems (pkgs: pkgs.nixpkgs-fmt);
    };
}
