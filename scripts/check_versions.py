#!/usr/bin/env python3
"""Verify that every Temporaless SDK and adapter uses one release version."""

from __future__ import annotations

import json
import os
import re
import sys
import tomllib
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
SEMVER_COMPONENT = r"(?:0|[1-9][0-9]*)"
SEMVER = re.compile(rf"{SEMVER_COMPONENT}\.{SEMVER_COMPONENT}\.{SEMVER_COMPONENT}")
OWNED_PACKAGES = {
    "temporaless",
    "temporaless-connectworkflow",
    "temporaless-indexstore",
    "temporaless-prefectcompat",
    "temporaless-temporalcompat",
}
PYPROJECTS = {
    "core/py/pyproject.toml": "temporaless",
    "adapters/py/connectworkflow/pyproject.toml": "temporaless-connectworkflow",
    "adapters/py/indexstore/pyproject.toml": "temporaless-indexstore",
    "adapters/py/prefectcompat/pyproject.toml": "temporaless-prefectcompat",
    "adapters/py/temporalcompat/pyproject.toml": "temporaless-temporalcompat",
}
LOCK_PACKAGES = {
    "core/py/uv.lock": {
        "temporaless",
        "temporaless-connectworkflow",
        "temporaless-indexstore",
    },
    "adapters/py/connectworkflow/uv.lock": {
        "temporaless",
        "temporaless-connectworkflow",
    },
    "adapters/py/indexstore/uv.lock": {
        "temporaless",
        "temporaless-indexstore",
    },
    "adapters/py/prefectcompat/uv.lock": {
        "temporaless",
        "temporaless-connectworkflow",
        "temporaless-prefectcompat",
    },
    "adapters/py/temporalcompat/uv.lock": {
        "temporaless",
        "temporaless-temporalcompat",
    },
}
PYTHON_REQUIREMENTS = {
    "core/py/pyproject.toml": {
        "temporaless-connectworkflow",
        "temporaless-indexstore",
    },
    "adapters/py/connectworkflow/pyproject.toml": {"temporaless"},
    "adapters/py/indexstore/pyproject.toml": {"temporaless"},
    "adapters/py/prefectcompat/pyproject.toml": {
        "temporaless",
        "temporaless-connectworkflow",
    },
    "adapters/py/temporalcompat/pyproject.toml": {"temporaless"},
}


def read_json(path: str) -> dict[str, Any]:
    return json.loads((ROOT / path).read_text())


def read_toml(path: str) -> dict[str, Any]:
    with (ROOT / path).open("rb") as file:
        return tomllib.load(file)


def captured(path: str, pattern: str) -> str:
    match = re.search(pattern, (ROOT / path).read_text(), re.MULTILINE)
    if match is None:
        raise ValueError(f"could not find version in {path}")
    return match.group(1)


def temporaless_requirements(pyproject: dict[str, Any]) -> dict[str, str]:
    requirements = list(pyproject["project"].get("dependencies", []))
    for group in pyproject["project"].get("optional-dependencies", {}).values():
        requirements.extend(group)
    for group in pyproject.get("dependency-groups", {}).values():
        requirements.extend(group)

    found: dict[str, str] = {}
    for requirement in requirements:
        match = re.fullmatch(r"(temporaless(?:-[a-z0-9]+)?)==(.+)", requirement)
        if match is not None and match.group(1) in OWNED_PACKAGES:
            name = match.group(1)
            if name in found:
                found[name] = f"<duplicate requirement including: {requirement}>"
            else:
                found[name] = match.group(2)
            continue
        package = re.match(r"(temporaless(?:-[a-z0-9]+)?)", requirement)
        if package is not None and package.group(1) in OWNED_PACKAGES:
            name = package.group(1)
            if name in found:
                found[name] = f"<duplicate requirement including: {requirement}>"
            else:
                found[name] = f"<not exact: {requirement}>"
    return found


def main() -> int:
    version = (ROOT / "VERSION").read_text().strip()
    errors: list[str] = []

    if SEMVER.fullmatch(version) is None:
        errors.append(f"VERSION must be MAJOR.MINOR.PATCH, got {version!r}")

    package = read_json("package.json")
    package_lock = read_json("package-lock.json")
    actual_versions: dict[str, object] = {
        "package.json": package["version"],
        "package-lock.json": package_lock["version"],
        'package-lock.json packages[""]': package_lock["packages"][""]["version"],
    }

    if "VERSION" not in package.get("files", []):
        errors.append("package.json files must include VERSION")
    if package.get("private") is not True:
        errors.append(
            "package.json must set private=true; Temporaless installs from Git only"
        )

    discovered_pyprojects = {"core/py/pyproject.toml"} | {
        path.relative_to(ROOT).as_posix()
        for path in (ROOT / "adapters/py").glob("*/pyproject.toml")
    }
    if discovered_pyprojects != set(PYPROJECTS):
        errors.append(
            f"Python project inventory is {sorted(discovered_pyprojects)}; "
            f"checker declares {sorted(PYPROJECTS)}"
        )
    discovered_locks = {"core/py/uv.lock"} | {
        path.relative_to(ROOT).as_posix()
        for path in (ROOT / "adapters/py").glob("*/uv.lock")
    }
    if discovered_locks != set(LOCK_PACKAGES):
        errors.append(
            f"Python lock inventory is {sorted(discovered_locks)}; "
            f"checker declares {sorted(LOCK_PACKAGES)}"
        )

    for path, expected_name in PYPROJECTS.items():
        pyproject = read_toml(path)
        if pyproject["project"]["name"] != expected_name:
            errors.append(
                f"{path} declares {pyproject['project']['name']!r}; expected {expected_name!r}"
            )
        actual_versions[path] = pyproject["project"]["version"]

        requirements = temporaless_requirements(pyproject)
        expected_requirements = PYTHON_REQUIREMENTS[path]
        if set(requirements) != expected_requirements:
            errors.append(
                f"{path} Temporaless requirements are {sorted(requirements)}; "
                f"expected {sorted(expected_requirements)}"
            )
        for dependency, dependency_version in requirements.items():
            if dependency_version != version:
                errors.append(
                    f"{path} requires {dependency} {dependency_version!r}; expected {version!r}"
                )

    for path, expected_names in LOCK_PACKAGES.items():
        lock = read_toml(path)
        packages = [item for item in lock["package"] if item["name"] in OWNED_PACKAGES]
        names = [item["name"] for item in packages]
        if len(names) != len(set(names)):
            errors.append(f"{path} contains duplicate Temporaless packages: {names}")
        if set(names) != expected_names:
            errors.append(
                f"{path} contains Temporaless packages {sorted(names)}; "
                f"expected {sorted(expected_names)}"
            )
        for item in packages:
            actual_versions[f"{path} package {item['name']}"] = item["version"]
            source = item.get("source")
            if (
                not isinstance(source, dict)
                or set(source) != {"editable"}
                or not isinstance(source["editable"], str)
                or not source["editable"]
            ):
                errors.append(
                    f"{path} package {item['name']} must use one editable path source; "
                    f"found {source!r}"
                )

    workspace = read_toml("Cargo.toml")
    actual_versions["Cargo.toml workspace.package"] = workspace["workspace"]["package"][
        "version"
    ]
    rust_package = read_toml("core/rs/temporaless/Cargo.toml")["package"]
    if rust_package.get("version") != {"workspace": True}:
        errors.append("core/rs/temporaless/Cargo.toml must inherit version.workspace")
    if rust_package.get("publish") is not False:
        errors.append(
            "core/rs/temporaless/Cargo.toml must set publish=false; "
            "Temporaless installs from Git only"
        )
    cargo_lock = read_toml("Cargo.lock")
    rust_entries = [
        item for item in cargo_lock["package"] if item["name"] == "temporaless"
    ]
    if len(rust_entries) != 1:
        errors.append(
            f"Cargo.lock must contain one temporaless package, found {len(rust_entries)}"
        )
    else:
        actual_versions["Cargo.lock package temporaless"] = rust_entries[0]["version"]

    for source, actual in actual_versions.items():
        if actual != version:
            errors.append(f"{source} has {actual!r}; expected {version!r}")

    module_path = captured("go.mod", r"^module\s+(\S+)$")
    if module_path != "github.com/jim-technologies/temporaless":
        errors.append(
            f"go.mod declares {module_path!r}; expected the repository root module"
        )

    ignored_dirs = {".flox", ".git", ".venv", "node_modules", "target"}
    nested_go_mods: list[Path] = []
    for directory, dirs, files in os.walk(ROOT):
        dirs[:] = [name for name in dirs if name not in ignored_dirs]
        path = Path(directory) / "go.mod"
        if "go.mod" in files and path != ROOT / "go.mod":
            nested_go_mods.append(path.relative_to(ROOT))
    if nested_go_mods:
        errors.append(f"nested Go modules are not allowed: {sorted(nested_go_mods)}")

    nested_npm_metadata = [
        path
        for path in ("core/ts/package.json", "core/ts/package-lock.json")
        if (ROOT / path).exists()
    ]
    if nested_npm_metadata:
        errors.append(
            f"nested npm metadata duplicates the root package: {nested_npm_metadata}"
        )

    readme = (ROOT / "README.md").read_text()
    for required_text in ("`VERSION`", "`vX.Y.Z`"):
        if required_text not in readme:
            errors.append(
                f"README.md must document the lockstep release marker {required_text}"
            )

    if os.environ.get("GITHUB_REF_TYPE") == "tag":
        expected_tag = f"v{version}"
        actual_tag = os.environ.get("GITHUB_REF_NAME")
        if actual_tag != expected_tag:
            errors.append(f"release tag is {actual_tag!r}; expected {expected_tag!r}")
        changelog_version = captured(
            "CHANGELOG.md", r"^## \[([0-9]+\.[0-9]+\.[0-9]+)\]"
        )
        if changelog_version != version:
            errors.append(
                f"tagged release CHANGELOG.md starts at {changelog_version!r}; expected {version!r}"
            )

    if errors:
        print("version consistency check failed:", file=sys.stderr)
        for error in errors:
            print(f"- {error}", file=sys.stderr)
        return 1

    print(f"versions aligned: {version} (tag v{version})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
