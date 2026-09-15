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
      # `pi --mode rpc`). pi-msg targets pi 0.84.0+ (see README); /abort needs
      # `clear_queue`, added in 0.84.4. Single source of truth — deployments
      # read it via passthru instead of pinning their own copy. Bumping pi:
      #   1. change piAgentVersion here,
      #   2. download the new tarball from the registry, verify its sha512
      #      against the registry's dist.integrity, replace pi-0.84.4.tgz below,
      #   3. copy the new tarball's npm-shrinkwrap.json and re-fill the six
      #      @earendil-works/pi-* integrity fields from each package's registry
      #      dist.integrity (upstream publishes them without one),
      #   4. refresh npmDepsHash: prefetch-npm-deps ./npm-shrinkwrap.json.
      piAgentVersion = "0.84.4";
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
          vendorHash = "sha256-9wjQDjRsdcuzuWMNar6BDtGWlbyqQUBY8mtv/I+zzU4=";
          # Single static bin from package main at the module root.
          meta = {
            description = "Bridge the Pi coding agent to XMPP.";
            homepage = "https://github.com/zachpmanson/pi-msg";
            mainProgram = "pi-msg";
            license = pkgs.lib.licenses.mit;
          };
          passthru.piAgentVersion = piAgentVersion;
        };
        # The pi coding agent itself, packaged from its published npm tarball
        # (ships a prebuilt dist/bundle — nothing to build). Putting pi in the
        # Nix store instead of `npm install -g` in a home-manager activation
        # makes deploys atomic: a switch swaps store paths (old and new coexist
        # until GC), so a pi bump can no longer race a pi-msg service restart
        # and crash every bridge with ENOENT dark.json (2026-09-04, all five
        # bridges). The tarball is vendored (pi-0.84.4.tgz) — see the comment on
        # piAgentVersion for the bump procedure. Vendoring also keeps the build
        # hermetic: naboo's build sandbox has no /etc/resolv.conf, so fetching
        # the tarball by URL inside a derivation fails or, worse, gets served a
        # stale/corrupted copy (2026-09-05: two deterministic hash mismatches on
        # naboo). The extracted tree is produced by pi-src below; npm-shrinkwrap.json
        # is the lockfile the tarball ships, patched the same way (see above).
        pi-src = pkgs.runCommand "pi-coding-agent-${piAgentVersion}-src" { src = ./pi-0.84.4.tgz; } ''
          mkdir -p $out
          tar xzf "$src" -C "$out" --strip-components=1
        '';
        pi = pkgs.buildNpmPackage {
          pname = "pi-coding-agent";
          version = piAgentVersion;
          src = pi-src;
          npmDepsHash = "sha256-X+T52/TtQ3RyVaZf66/ApGCpfSCLQDiToFqR2v8/Y5A=";
          npmDepsFetcherVersion = 2;
          # The tarball ships prebuilt dist/ — running `npm run build` would
          # try to recompile from source (tsgo) and fail without the toolchain.
          dontNpmBuild = true;
          # npmConfigHook validates src's npm-shrinkwrap.json against the
          # prefetched cache lock; the in-repo (integrity-patched) copy is the
          # source of truth, so lay it over the tarball's before configuring.
          # npm ci also resolves the TOP-LEVEL devDependencies of the published
          # package.json (@types/cross-spawn 6.0.6, typescript, vitest, ...) —
          # none of which exist in npm-shrinkwrap.json (lockfile v3 omits dev-only
          # entries), so npm tries the registry and fails ENOTCACHED offline.
          # Nothing runs any script (dontNpmBuild), so drop devDependencies
          # before npm ever looks at them.
          postPatch = ''
            cp ${./npm-shrinkwrap.json} npm-shrinkwrap.json
            ${pkgs.jq}/bin/jq 'del(.devDependencies)' package.json > package.json.tmp && mv package.json.tmp package.json
          '';
          # pi requires node >= 20.6 (its pi-ai dep requires >= 22.19); match
          # the node the deployment runs today.
          nodejs = pkgs.nodejs_22;
          meta = {
            description = "The pi coding agent CLI (pi --mode rpc), packaged from its npm tarball.";
            homepage = "https://github.com/earendil-works/pi-mono";
            mainProgram = "pi";
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
          ];
          shellHook = ''
            echo "pi-msg dev shell — $(go version)"
          '';
        };
      });

      formatter = forAllSystems (pkgs: pkgs.nixpkgs-fmt);
    };
}
