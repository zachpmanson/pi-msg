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
      # The pi-coding-agent version this bridge requires at runtime (spawned as
      # `pi --mode rpc`). Pi 1.5.2 supplies the native GPT-6.2 Sol catalog;
      # runtime/extension/XMPP compatibility is gated before fleet activation.
      # Single source of truth — deployments read this via passthru.
      piAgentVersion = "1.5.2";
      forAllSystems = f: nixpkgs.lib.genAttrs systems (system: f (import nixpkgs { inherit system; }));
    in
    {
      packages = forAllSystems (pkgs: rec {
        default = pi-msg;
        pi-msg = pkgs.buildGoModule {
          pname = "pi-msg";
          version = "0.3.0";
          subPackages = [ "cmd/pi-msg" ];
          src = ./.;
          # Hash of the Go module dependencies. Bump when go.mod/go.sum change:
          # set to pkgs.lib.fakeHash, run `nix build`, and copy the reported hash.
          vendorHash = "sha256-9wjQDjRsdcuzuWMNar6BDtGWlbyqQUBY8mtv/I+zzU4=";
          # Single static bin from cmd/pi-msg.
          meta = {
            description = "Bridge the Pi coding agent to XMPP.";
            homepage = "https://github.com/zachpmanson/pi-msg";
            mainProgram = "pi-msg";
            license = pkgs.lib.licenses.mit;
          };
          passthru.piAgentVersion = piAgentVersion;
        };
      });

      devShells = forAllSystems (pkgs: {
        default = pkgs.mkShell {
          packages = [
            pkgs.go
            pkgs.gopls
            pkgs.nodejs_22
          ];
          shellHook = ''
            echo "pi-msg dev shell — $(go version)"
          '';
        };
      });

      formatter = forAllSystems (pkgs: pkgs.nixpkgs-fmt);
    };
}
