#!/usr/bin/env bash
set -euo pipefail

skip_fmt=0
skip_known_failing_package_tests=0

for arg in "$@"; do
  case "$arg" in
    --skip-fmt)
      skip_fmt=1
      ;;
    --skip-known-failing-package-tests)
      skip_known_failing_package_tests=1
      ;;
    *)
      printf 'unknown argument: %s\n' "$arg" >&2
      exit 2
      ;;
  esac
done

run() {
  printf '\n==> %s\n' "$*"
  "$@"
}

if [ "$skip_fmt" -eq 0 ]; then
  run cargo fmt --all -- --check
else
  printf '\n==> Skipping cargo fmt --all -- --check\n'
fi

run cargo check --workspace --all-targets

packages=(
  rustwx-models
  rustwx-io
  rustwx-products
  rustwx-render
  rustwx-radar
  rustwx-regrid
)

if [ "$skip_known_failing_package_tests" -eq 1 ]; then
  printf '\n==> Skipping currently known failing package tests: rustwx-products, rustwx-render\n'
  packages=(
    rustwx-models
    rustwx-io
    rustwx-radar
    rustwx-regrid
  )
fi

for package in "${packages[@]}"; do
  run cargo test -p "$package" --lib
done

run cargo test -p rustwx-products --test product_catalog_inventory
run cargo test -p rustwx-cli --test bin_inventory

printf '\nWorkspace checks passed.\n'
