#!/usr/bin/env python3
"""Check that every place that carries the library version agrees.

Run from anywhere: python3 tools/check_versions.py [--tag X.Y.Z]. Exit
status 0 when all agree, 1 otherwise. With --tag (the release workflow passes
the pushed tag) the version must equal the tag and the CHANGELOG head section
must already be the release (not "Unreleased"). The version lives in:

- data_tamer_cpp/CMakeLists.txt  project(... VERSION x.y.z): the shared library
  VERSION and SOVERSION (= major) and DATA_TAMER_VERSION derive from it;
- data_tamer_cpp/package.xml and data_tamer_msgs/package.xml  <version>;
- data_tamer_cpp/conanfile.py  version = "x.y.z";
- python/data_tamer_parser.py  __version__;
- data_tamer_cpp/CHANGELOG.rst  the first section is "Unreleased" (the release
  commit renames it) or carries the same number.

The links of the Python package (python/README.md, python/pyproject.toml) to
the wire format point at the tag of this version, so that the PyPI page of a
release documents that release.
"""
import argparse
import pathlib
import re
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent


def first_match(relative, pattern, flags=0):
    text = (ROOT / relative).read_text(encoding="utf-8")
    match = re.search(pattern, text, flags)
    if not match:
        sys.exit(f"{relative}: pattern {pattern!r} not found")
    return match.group(1)


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--tag", help="release tag being published (X.Y.Z)")
    args = parser.parse_args()

    versions = {
        "data_tamer_cpp/CMakeLists.txt": first_match(
            "data_tamer_cpp/CMakeLists.txt",
            r"^\s*project\(\s*data_tamer_cpp\s+VERSION\s+(\d+\.\d+\.\d+)", re.M),
        "data_tamer_cpp/package.xml": first_match(
            "data_tamer_cpp/package.xml", r"<version>([^<]+)</version>"),
        "data_tamer_msgs/package.xml": first_match(
            "data_tamer_msgs/package.xml", r"<version>([^<]+)</version>"),
        "data_tamer_cpp/conanfile.py": first_match(
            "data_tamer_cpp/conanfile.py", r'^\s*version\s*=\s*"([^"]+)"', re.M),
        "python/data_tamer_parser.py": first_match(
            "python/data_tamer_parser.py", r'^__version__\s*=\s*"([^"]+)"', re.M),
    }
    expected = versions["data_tamer_cpp/CMakeLists.txt"]
    ok = True
    if args.tag is not None and args.tag != expected:
        print(f"tag {args.tag} != version {expected}")
        ok = False
    for name, version in versions.items():
        if version != expected:
            print(f"{name}: {version}, expected {expected}")
            ok = False

    changelog = first_match(
        "data_tamer_cpp/CHANGELOG.rst",
        r"^\^+\n.*\n\^+\n\n(.+)\n-+\n", re.M)
    head = changelog.split()[0]
    if args.tag is not None and head != expected:
        print(f"data_tamer_cpp/CHANGELOG.rst: head section is {head!r}, "
              f"expected {expected} in a release")
        ok = False
    elif head != "Unreleased" and head != expected:
        print(f"data_tamer_cpp/CHANGELOG.rst: head section is {head!r}, "
              f"expected 'Unreleased' or {expected}")
        ok = False

    link = f"github.com/PlotJuggler/data_tamer/blob/{expected}/docs/wire_format.md"
    for relative in ("python/README.md", "python/pyproject.toml"):
        text = (ROOT / relative).read_text(encoding="utf-8")
        if "docs/wire_format.md" in text and link not in text:
            print(f"{relative}: the wire format link must point at {link}")
            ok = False

    if ok:
        print(f"all versions are {expected}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
