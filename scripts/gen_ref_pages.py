"""Generate mkdocstrings reference pages for public Atlas modules."""

from __future__ import annotations

import tomllib
from pathlib import Path

import mkdocs_gen_files  # ty: ignore[unresolved-import]

ROOT = Path(__file__).resolve().parent.parent
WORKSPACE_PYPROJECT = ROOT / "pyproject.toml"
REFERENCE_ROOT = Path("reference/api")
SUMMARY_PATH = REFERENCE_ROOT / "SUMMARY.md"
OVERVIEW_ENTRY = "* [API Overview](index.md)\n"


def _workspace_src_dirs() -> list[Path]:
    data = tomllib.loads(WORKSPACE_PYPROJECT.read_text(encoding="utf-8"))
    workspace_members = data["tool"]["uv"]["workspace"]["members"]

    src_dirs: list[Path] = []
    for member in workspace_members:
        src_dir = ROOT / member / "src"
        if src_dir.is_dir():
            src_dirs.append(src_dir)
    return src_dirs


def _is_public_module(parts: tuple[str, ...]) -> bool:
    return all(not part.startswith("_") for part in parts)


nav = mkdocs_gen_files.Nav()

for src_dir in _workspace_src_dirs():
    for path in sorted(src_dir.rglob("*.py")):
        module_path = path.relative_to(src_dir).with_suffix("")
        parts = tuple(module_path.parts)

        if parts[-1] == "__main__":
            continue

        if parts[-1] == "__init__":
            public_parts = parts[:-1]
            if not public_parts or not _is_public_module(public_parts):
                continue
            doc_path = Path(*public_parts) / "index.md"
            ident_parts = public_parts
        else:
            if not _is_public_module(parts):
                continue
            doc_path = Path(*parts).with_suffix(".md")
            ident_parts = parts

        identifier = ".".join(ident_parts)
        full_doc_path = REFERENCE_ROOT / doc_path
        nav[ident_parts] = doc_path.as_posix()

        with mkdocs_gen_files.open(full_doc_path, "w") as file:
            file.write(f"::: {identifier}\n")

        mkdocs_gen_files.set_edit_path(full_doc_path, path.relative_to(ROOT))

with mkdocs_gen_files.open(SUMMARY_PATH, "w") as nav_file:
    nav_file.write(OVERVIEW_ENTRY)
    nav_file.writelines(nav.build_literate_nav())
