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
INVARIANT_PACKAGE = "@jim-technologies/invariant-protocol"
INVARIANT_REPOSITORY = "github.com/jim-technologies/invariantprotocol.git"
INVARIANT_SPEC = re.compile(
    rf"git\+https://{re.escape(INVARIANT_REPOSITORY)}#([0-9a-f]{{40}})"
)
INVARIANT_ALLOW_SCRIPT_PREFIX = "github:jim-technologies/invariantprotocol#"
OPENDAL_REPOSITORY = "https://github.com/apache/opendal.git"
FULL_GIT_SHA = re.compile(r"[0-9a-f]{40}")
LICENSE = "Apache-2.0"
PROJECT_URLS = {
    "Homepage": "https://github.com/jim-technologies/temporaless",
    "Repository": "https://github.com/jim-technologies/temporaless",
    "Issues": "https://github.com/jim-technologies/temporaless/issues",
}
NPM_HOMEPAGE = f"{PROJECT_URLS['Homepage']}#readme"
NPM_REPOSITORY = {
    "type": "git",
    "url": "git+https://github.com/jim-technologies/temporaless.git",
}
NPM_BUGS = {"url": PROJECT_URLS["Issues"]}
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
PY_TYPED_MARKERS = {
    "core/py/pyproject.toml": "core/py/src/temporaless/py.typed",
    "adapters/py/connectworkflow/pyproject.toml": (
        "adapters/py/connectworkflow/src/temporaless_connectworkflow/py.typed"
    ),
    "adapters/py/indexstore/pyproject.toml": (
        "adapters/py/indexstore/src/temporaless_indexstore/py.typed"
    ),
    "adapters/py/prefectcompat/pyproject.toml": (
        "adapters/py/prefectcompat/src/temporaless_prefectcompat/py.typed"
    ),
    "adapters/py/temporalcompat/pyproject.toml": (
        "adapters/py/temporalcompat/src/temporaless_temporalcompat/py.typed"
    ),
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
    for field, actual, expected in (
        ("license", package.get("license"), LICENSE),
        ("homepage", package.get("homepage"), NPM_HOMEPAGE),
        ("repository", package.get("repository"), NPM_REPOSITORY),
        ("bugs", package.get("bugs"), NPM_BUGS),
    ):
        if actual != expected:
            errors.append(f"package.json {field} is {actual!r}; expected {expected!r}")

    invariant_dependency = package.get("dependencies", {}).get(INVARIANT_PACKAGE)
    invariant_match = (
        INVARIANT_SPEC.fullmatch(invariant_dependency)
        if isinstance(invariant_dependency, str)
        else None
    )
    invariant_sha = invariant_match.group(1) if invariant_match is not None else None
    if invariant_sha is None:
        errors.append(
            f"package.json must pin {INVARIANT_PACKAGE} to one full HTTPS Git SHA; "
            f"found {invariant_dependency!r}"
        )

    locked_root_dependency = (
        package_lock.get("packages", {})
        .get("", {})
        .get("dependencies", {})
        .get(INVARIANT_PACKAGE)
    )
    if locked_root_dependency != invariant_dependency:
        errors.append(
            f"package-lock.json root {INVARIANT_PACKAGE} dependency is "
            f"{locked_root_dependency!r}; expected {invariant_dependency!r}"
        )

    locked_invariant = package_lock.get("packages", {}).get(
        f"node_modules/{INVARIANT_PACKAGE}"
    )
    if not isinstance(locked_invariant, dict):
        errors.append(f"package-lock.json is missing node_modules/{INVARIANT_PACKAGE}")
    else:
        if locked_invariant.get("resolved") != invariant_dependency:
            errors.append(
                f"package-lock.json locked {INVARIANT_PACKAGE} source is "
                f"{locked_invariant.get('resolved')!r}; expected {invariant_dependency!r}"
            )
        locked_version = locked_invariant.get("version")
        if (
            not isinstance(locked_version, str)
            or SEMVER.fullmatch(locked_version) is None
        ):
            errors.append(
                f"package-lock.json locked {INVARIANT_PACKAGE} version must be SemVer; "
                f"found {locked_version!r}"
            )
        integrity = locked_invariant.get("integrity")
        if not isinstance(integrity, str) or not integrity.startswith("sha512-"):
            errors.append(
                f"package-lock.json locked {INVARIANT_PACKAGE} must have sha512 integrity"
            )
    allow_scripts = package.get("allowScripts", {})
    invariant_script_keys = (
        {
            key
            for key in allow_scripts
            if isinstance(key, str) and key.startswith(INVARIANT_ALLOW_SCRIPT_PREFIX)
        }
        if isinstance(allow_scripts, dict)
        else set()
    )
    expected_invariant_script_key = (
        f"{INVARIANT_ALLOW_SCRIPT_PREFIX}{invariant_sha}"
        if invariant_sha is not None
        else None
    )
    if (
        expected_invariant_script_key is None
        or invariant_script_keys != {expected_invariant_script_key}
        or allow_scripts.get(expected_invariant_script_key) is not True
    ):
        errors.append(
            "package.json allowScripts must enable exactly the pinned Invariant "
            f"Protocol SHA; found {sorted(invariant_script_keys)}"
        )
    if (
        not isinstance(allow_scripts, dict)
        or allow_scripts.get("protobufjs") is not False
    ):
        errors.append("package.json allowScripts must explicitly deny protobufjs")

    discovered_pyprojects = {"core/py/pyproject.toml"} | {
        path.relative_to(ROOT).as_posix()
        for path in (ROOT / "adapters/py").glob("*/pyproject.toml")
    }
    if discovered_pyprojects != set(PYPROJECTS):
        errors.append(
            f"Python project inventory is {sorted(discovered_pyprojects)}; "
            f"checker declares {sorted(PYPROJECTS)}"
        )
    if set(PY_TYPED_MARKERS) != set(PYPROJECTS):
        errors.append(
            f"py.typed marker inventory is {sorted(PY_TYPED_MARKERS)}; "
            f"expected {sorted(PYPROJECTS)}"
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
        project = pyproject["project"]
        if project["name"] != expected_name:
            errors.append(
                f"{path} declares {project['name']!r}; expected {expected_name!r}"
            )
        if project.get("license") != LICENSE:
            errors.append(
                f"{path} license is {project.get('license')!r}; expected {LICENSE!r}"
            )
        if project.get("urls") != PROJECT_URLS:
            errors.append(
                f"{path} project URLs are {project.get('urls')!r}; "
                f"expected {PROJECT_URLS!r}"
            )
        if "Private :: Do Not Upload" not in project.get("classifiers", []):
            errors.append(
                f"{path} must declare the Private :: Do Not Upload classifier"
            )
        marker = ROOT / PY_TYPED_MARKERS[path]
        if not marker.is_file():
            errors.append(
                f"{path} typed package marker is missing: {marker.relative_to(ROOT)}"
            )
        actual_versions[path] = project["version"]

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
    rust_manifest = read_toml("core/rs/temporaless/Cargo.toml")
    rust_package = rust_manifest["package"]
    if rust_package.get("version") != {"workspace": True}:
        errors.append("core/rs/temporaless/Cargo.toml must inherit version.workspace")
    if rust_package.get("publish") is not False:
        errors.append(
            "core/rs/temporaless/Cargo.toml must set publish=false; "
            "Temporaless installs from Git only"
        )
    opendal_dependency = rust_manifest.get("dependencies", {}).get("opendal")
    opendal_sha: str | None = None
    if not isinstance(opendal_dependency, dict):
        errors.append(
            "core/rs/temporaless/Cargo.toml must pin opendal to an immutable Git commit"
        )
    else:
        if opendal_dependency.get("git") != OPENDAL_REPOSITORY:
            errors.append(
                "core/rs/temporaless/Cargo.toml opendal git source is "
                f"{opendal_dependency.get('git')!r}; expected {OPENDAL_REPOSITORY!r}"
            )
        candidate_sha = opendal_dependency.get("rev")
        if (
            not isinstance(candidate_sha, str)
            or FULL_GIT_SHA.fullmatch(candidate_sha) is None
        ):
            errors.append(
                "core/rs/temporaless/Cargo.toml opendal rev must be one full Git SHA"
            )
        else:
            opendal_sha = candidate_sha
        if opendal_dependency.get("default-features") is not False:
            errors.append(
                "core/rs/temporaless/Cargo.toml opendal must disable default features"
            )
        if opendal_dependency.get("features") != ["services-fs"]:
            errors.append(
                "core/rs/temporaless/Cargo.toml opendal must enable only services-fs"
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
    expected_opendal_source = (
        f"git+{OPENDAL_REPOSITORY}?rev={opendal_sha}#{opendal_sha}"
        if opendal_sha is not None
        else None
    )
    for package_name in ("opendal", "opendal-core", "opendal-service-fs"):
        entries = [
            item for item in cargo_lock["package"] if item["name"] == package_name
        ]
        if len(entries) != 1:
            errors.append(
                f"Cargo.lock must contain one {package_name} package, found {len(entries)}"
            )
        elif entries[0].get("source") != expected_opendal_source:
            errors.append(
                f"Cargo.lock package {package_name} source is "
                f"{entries[0].get('source')!r}; expected {expected_opendal_source!r}"
            )

    for source, actual in actual_versions.items():
        if actual != version:
            errors.append(f"{source} has {actual!r}; expected {version!r}")

    module_path = captured("go.mod", r"^module\s+(\S+)$")
    if module_path != "github.com/jim-technologies/temporaless":
        errors.append(
            f"go.mod declares {module_path!r}; expected the repository root module"
        )
    go_version = captured("go.mod", r"^go\s+(\S+)$")
    if SEMVER.fullmatch(go_version) is None:
        errors.append(
            f"go.mod must declare an exact Go patch version, got {go_version!r}"
        )
    flox_manifest = read_toml(".flox/env/manifest.toml")
    expected_toolchain = f"go{go_version}+auto"
    actual_toolchain = flox_manifest.get("vars", {}).get("GOTOOLCHAIN")
    if actual_toolchain != expected_toolchain:
        errors.append(
            f".flox/env/manifest.toml GOTOOLCHAIN is {actual_toolchain!r}; "
            f"expected {expected_toolchain!r}"
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
    for path in ("README.md", "docs/sdks.md", "core/ts/README.md"):
        for line_number, line in enumerate((ROOT / path).read_text().splitlines(), 1):
            if line.strip().startswith("npm install") and "--allow-git=all" not in line:
                errors.append(
                    f"{path}:{line_number} Git npm install must pass --allow-git=all"
                )

    if os.environ.get("GITHUB_REF_TYPE") == "tag":
        expected_tag = f"v{version}"
        actual_tag = os.environ.get("GITHUB_REF_NAME")
        if actual_tag != expected_tag:
            errors.append(f"release tag is {actual_tag!r}; expected {expected_tag!r}")
        changelog = (ROOT / "CHANGELOG.md").read_text()
        unreleased = re.search(
            r"^## \[Unreleased\]\s*(.*?)^## \[",
            changelog,
            re.MULTILINE | re.DOTALL,
        )
        if unreleased is None:
            errors.append("CHANGELOG.md must start with an [Unreleased] section")
        elif unreleased.group(1).strip():
            errors.append(
                "tagged release CHANGELOG.md must keep the [Unreleased] section empty"
            )
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
