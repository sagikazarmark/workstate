[private]
default:
  @just --list

[private]
files:
  @mkdir -p var/files

run: files
  granian --interface asginl src/yt_dlp_restate.run:app --host 0.0.0.0 --port 9080 --reload

# tag and release a new version
release bump='patch':
  #!/usr/bin/env bash
  set -euo pipefail

  git checkout main > /dev/null 2>&1
  git diff-index --quiet HEAD || (echo "Git directory is dirty" && exit 1)

  version=$(semver bump {{bump}} $(git tag --sort=v:refname | grep -v beta | tail -1 || echo "v0.0.0"))
  tag="v${version}"

  echo "Tagging with version ${version}"
  read -n 1 -p "Proceed (y/N)? " answer
  echo

  case ${answer:0:1} in
      y|Y )
      ;;
      * )
          echo "Aborting"
          exit 1
      ;;
  esac

  git tag -m "Release ${version}" $tag
  git push origin $tag
