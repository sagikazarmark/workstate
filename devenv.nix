{ pkgs, ... }:

{
  packages = with pkgs; [
    just
    semver-tool
  ];

  languages = {
    python = {
      enable = true;

      uv = {
        enable = true;
        sync = {
          enable = true;
          allExtras = true;
          allGroups = true;
        };
      };
      venv.enable = true;
    };
  };
}
