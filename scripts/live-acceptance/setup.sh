#!/bin/sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
install_bin=${INSTALL_BIN:-$HOME/.local/bin}
plugin_config=$(herdr plugin config-dir herdr-harness-coordinator)

command -v cargo >/dev/null
command -v herdr >/dev/null
command -v omp >/dev/null
command -v codex >/dev/null

cargo build --release --locked --manifest-path "$repo_root/Cargo.toml"
mkdir -p "$install_bin"
ln -sfn "$repo_root/target/release/herdr-harness-coordinator" \
  "$install_bin/herdr-harness-coordinator"

herdr plugin unlink herdr-harness-coordinator >/dev/null 2>&1 || true
herdr plugin link "$repo_root/plugin/herdr-harness-coordinator"
mkdir -p "$plugin_config/profiles"
cp "$repo_root/scripts/live-acceptance/profiles/"*.toml "$plugin_config/profiles/"
herdr server reload-config

herdr --version
omp --version
codex --version
printf 'binary=%s\nplugin=%s\nprofiles=%s\n' \
  "$install_bin/herdr-harness-coordinator" \
  "$repo_root/plugin/herdr-harness-coordinator" \
  "$plugin_config/profiles"
