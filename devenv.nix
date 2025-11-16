{ pkgs, ... }:

{
  packages = with pkgs; [
    just
    semver-tool
  ];

  languages = {
    python = {
      enable = true;

      uv.enable = true;
      venv.enable = true;
    };
  };
}
