{
  description = "storeroon — tooling for music collection management";

  inputs = {
    # nixos-24.11 is stable and unaffected by the sphinx-9.1/python311 breakage
    # that hit nixos-unstable in early 2026. We use python312 here because
    # sphinx dropped python311 support, breaking several transitive deps on 3.11.
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = nixpkgs.legacyPackages.${system};

        pythonEnv = pkgs.python312.withPackages (ps: with ps; [
          rich
          mutagen
          jinja2
        ]);

      in
      {
        devShells.default = pkgs.mkShell {
          name = "storeroon";

          packages = [
            pythonEnv
            pkgs.ffmpeg
            pkgs.flac
          ];

          shellHook = ''
            echo ""
            echo "🎬  storeroon dev environment ready"
            echo ""
            echo "  Python : $(python --version)"
            echo "  ffmpeg : $(ffmpeg -version 2>&1 | head -1)"
            echo "  flac: $(flac -version 2>&1 | head -1)"
            echo ""
            echo "  Run:  python -m storeroon"
            echo ""
          '';
        };
      }
    );
}
