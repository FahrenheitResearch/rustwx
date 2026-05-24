"""Repo-local launcher for the focused RustWx model-map app."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


def _load_repo_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def main() -> int:
    try:
        import rustwx  # noqa: F401
    except Exception as exc:  # pragma: no cover - user-facing launcher guard
        print(f"Unable to import the installed rustwx Python package: {exc}", file=sys.stderr)
        print("Install the RustWx Python package, then rerun this launcher.", file=sys.stderr)
        return 2

    repo_root = Path(__file__).resolve().parents[1]
    package_dir = repo_root / "crates" / "rustwx-python" / "python" / "rustwx"
    try:
        _load_repo_module("rustwx.studio", package_dir / "studio.py")
    except Exception as exc:
        print(f"Unable to load repo-local Studio backend: {exc}", file=sys.stderr)
        return 2
    module_path = repo_root / "crates" / "rustwx-python" / "python" / "rustwx" / "model_maps.py"
    try:
        module = _load_repo_module("rustwx.model_maps", module_path)
    except Exception as exc:
        print(f"Unable to load {module_path}: {exc}", file=sys.stderr)
        return 2
    return int(module.run_cli())


if __name__ == "__main__":
    raise SystemExit(main())
