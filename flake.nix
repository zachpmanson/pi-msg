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
        pi-msg = pkgs.buildNpmPackage {
          pname = "pi-msg";
          version = "0.2.0";
          src = ./.;
          npmDepsHash = "sha256-gYx/oiDnnYZDvREj/Ff7ZhHe8cntu7lNVWedO75NWd0=";
          # Node >= 22.18 runs the TypeScript entry directly (built-in type stripping);
          # no build step. patchShebangs pins this node into the `pi-msg` bin's shebang.
          nodejs = pkgs.nodejs_22;
          dontNpmBuild = true;
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
            pkgs.nodejs_22
            pkgs.typescript
          ];
          shellHook = ''
            echo "pi-msg dev shell — node $(node --version)"
          '';
        };
      });

      formatter = forAllSystems (pkgs: pkgs.nixpkgs-fmt);
    };
}
