#!/usr/bin/env python3
"""Synchronize every Temporaless package mirror from one release version."""

from __future__ import annotations

import json
import os
import re
import stat
import sys
import tempfile
from pathlib import Path
from typing import Any

import check_versions

ROOT = Path(__file__).resolve().parents[1]


def write_text(path: Path, value: str) -> None:
    mode = stat.S_IMODE(path.stat().st_mode) if path.exists() else 0o644
    descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(descriptor, "w") as file:
            file.write(value)
        os.chmod(temporary, mode)
        os.replace(temporary, path)
    except BaseException:
        Path(temporary).unlink(missing_ok=True)
        raise


def write_json(path: Path, value: dict[str, Any]) -> None:
    write_text(path, json.dumps(value, indent=2) + "\n")


def replace_once(text: str, pattern: str, replacement: str, path: Path) -> str:
    updated, count = re.subn(pattern, replacement, text, count=1, flags=re.MULTILINE)
    if count != 1:
        raise ValueError(f"expected one version field in {path}, found {count}")
    return updated


def update_pyproject(path: Path, version: str) -> None:
    text = replace_once(
        path.read_text(),
        r'^version = "[0-9]+\.[0-9]+\.[0-9]+"$',
        f'version = "{version}"',
        path,
    )
    text = re.sub(
        r'^(\s+"temporaless(?:-[a-z0-9]+)?)(?:==[0-9]+\.[0-9]+\.[0-9]+)?(",)$',
        rf"\g<1>=={version}\g<2>",
        text,
        flags=re.MULTILINE,
    )
    write_text(path, text)


def update_lock_packages(path: Path, version: str, names: set[str]) -> None:
    blocks = re.split(r"(?=^\[\[package\]\]$)", path.read_text(), flags=re.MULTILINE)
    updated_blocks: list[str] = []
    found: set[str] = set()
    for block in blocks:
        name_match = re.search(r'^name = "([^"]+)"$', block, re.MULTILINE)
        if name_match is not None and name_match.group(1) in names:
            name = name_match.group(1)
            block = replace_once(
                block,
                r'^version = "[0-9]+\.[0-9]+\.[0-9]+"$',
                f'version = "{version}"',
                path,
            )
            found.add(name)
        updated_blocks.append(block)
    if found != names:
        raise ValueError(f"{path} contained {sorted(found)}; expected {sorted(names)}")
    text = "".join(updated_blocks)
    text = re.sub(
        r'(\{ name = "temporaless(?:-[a-z0-9]+)?", specifier = "==)'
        r'[0-9]+\.[0-9]+\.[0-9]+(" \})',
        rf"\g<1>{version}\g<2>",
        text,
    )
    write_text(path, text)


def main() -> int:
    if len(sys.argv) != 2 or check_versions.SEMVER.fullmatch(sys.argv[1]) is None:
        print(
            "usage: python3 scripts/set_version.py MAJOR.MINOR.PATCH", file=sys.stderr
        )
        return 2
    version = sys.argv[1]

    write_text(ROOT / "VERSION", f"{version}\n")

    package_path = ROOT / "package.json"
    package = json.loads(package_path.read_text())
    package["version"] = version
    write_json(package_path, package)

    package_lock_path = ROOT / "package-lock.json"
    package_lock = json.loads(package_lock_path.read_text())
    package_lock["version"] = version
    package_lock["packages"][""]["version"] = version
    write_json(package_lock_path, package_lock)

    for path in check_versions.PYPROJECTS:
        update_pyproject(ROOT / path, version)
    for path, names in check_versions.LOCK_PACKAGES.items():
        update_lock_packages(ROOT / path, version, names)

    cargo_path = ROOT / "Cargo.toml"
    cargo_text = replace_once(
        cargo_path.read_text(),
        r'(^\[workspace\.package\]\nversion = ")[0-9]+\.[0-9]+\.[0-9]+("$)',
        rf"\g<1>{version}\g<2>",
        cargo_path,
    )
    write_text(cargo_path, cargo_text)
    update_lock_packages(ROOT / "Cargo.lock", version, {"temporaless"})

    return check_versions.main()


if __name__ == "__main__":
    raise SystemExit(main())
