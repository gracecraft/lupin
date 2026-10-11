{
  description = "lupin -- model routing and slot leases for multi-machine delegation loops";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/8ce4ef6cb6f871616146b9fe26d2a5ae594e94fe";
  };

  outputs =
    { self, nixpkgs }:
    let
      systems = [ "aarch64-linux" "x86_64-linux" "aarch64-darwin" ];
      forEachSystem = f: nixpkgs.lib.genAttrs systems f;
      mkLupin =
        pkgs:
        pkgs.python3Packages.buildPythonApplication {
          pname = "lupin";
          version = "0.1.0";
          pyproject = true;
          src = self;
          build-system = [ pkgs.python3Packages.setuptools ];
          # python3Packages.redis is the `redis` backend's only runtime
          # dependency (issue #210).
          dependencies = [ pkgs.python3Packages.redis ];
          doCheck = false;
          # `lupin serve` shells out to these fixed read-only probes and
          # runs them from PATH, so they belong on the wrapper's PATH.
          # `systemd` and `chromium` are Linux-only and this flake builds for darwin too.
          # `chromium` takes the periodic debrief screenshots.
          # `omp` is deliberately absent: it is not in nixpkgs. The caller
          # puts it on PATH (ghostbook.nix's ai-skills-claude module ships
          # it), and the usage page already reports a row as unavailable
          # when the binary is missing rather than failing.
          makeWrapperArgs = [
            "--prefix"
            "PATH"
            ":"
            (nixpkgs.lib.makeBinPath (
              [
                pkgs.gh
                pkgs.git
                pkgs.tmux
              ]
              ++ nixpkgs.lib.optionals pkgs.stdenv.isLinux [ pkgs.systemd pkgs.chromium ]
            ))
          ];
          meta = {
            description = "Model routing and slot leases for multi-machine delegation loops";
            mainProgram = "lupin";
          };
        };
    in
    {
      packages = forEachSystem (
        system:
        let
          pkgs = nixpkgs.legacyPackages.${system};
        in
        {
          default = mkLupin pkgs;
        }
      );

      devShells = forEachSystem (
        system:
        let
          pkgs = nixpkgs.legacyPackages.${system};
        in
        {
          default = pkgs.mkShell {
            packages = [
              (pkgs.python3.withPackages (pythonPackages: [
                pythonPackages.pytest
                pythonPackages.redis
              ]))
              # tests/conftest.py starts `redis-server` from PATH.
              pkgs.redis
            ];
          };
        }
      );

      checks = forEachSystem (
        system:
        let
          pkgs = nixpkgs.legacyPackages.${system};
        in
        {
          default = pkgs.runCommand "lupin-tests" {
            src = self;
            package = self.packages.${system}.default;
            nativeBuildInputs = [
              (pkgs.python3.withPackages (pythonPackages: [
                pythonPackages.pytest
                pythonPackages.redis
              ]))
              pkgs.redis
              # `roadmap._git_remote_url` shells out to `git`, and the
              # wrapper's PATH does not reach this builder -- the sandbox
              # only sees nativeBuildInputs.
              pkgs.git
            ];
          } ''
            test -x "$package/bin/lupin"
            "$package/bin/lupin" --help >/dev/null
            mkdir source
            cp -R "$src"/. source/
            chmod -R u+w source
            cd source
            pytest -q
            touch "$out"
          '';
        }
      );

      # Filled in by a later sub-issue, once lupin is a real dependency of a
      # fleet config (ghostbook.nix, lab.nix). No options yet -- there is no
      # consumer to write them against.
      nixosModules.default = { config, lib, pkgs, ... }: { };
    };
}
