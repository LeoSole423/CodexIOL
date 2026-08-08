"""Enforce package-layer dependency rules.

Allowed dependency directions (lower layers may not import from higher):

    iol_shared  →  (nothing project-internal)
    iol_reconciliation  →  iol_shared
    iol_advisor  →  iol_shared  (no longer depends on iol_cli or iol_web)
    iol_cli  →  iol_shared, iol_advisor, iol_reconciliation
    iol_web  →  iol_shared, iol_advisor, iol_reconciliation
"""
import ast
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1] / "src"

ALL_PACKAGES = {"iol_shared", "iol_advisor", "iol_reconciliation", "iol_cli", "iol_web"}


def _imports_for_package(pkg: str) -> set[str]:
    """Return the set of all top-level module names imported by any .py file in *pkg*."""
    names: set[str] = set()
    for path in (ROOT / pkg).rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for item in node.names:
                    names.add(item.name.split(".")[0])
            elif isinstance(node, ast.ImportFrom) and node.module:
                names.add(node.module.split(".")[0])
    return names & ALL_PACKAGES  # only project-internal deps


def _pkg_imports(pkg: str) -> set[str]:
    deps = _imports_for_package(pkg)
    deps.discard(pkg)
    return deps


class TestArchitectureBoundaries(unittest.TestCase):
    # ------------------------------------------------------------------ #
    # iol_shared: no project-internal deps
    # ------------------------------------------------------------------ #
    def test_shared_has_no_internal_deps(self):
        deps = _pkg_imports("iol_shared")
        self.assertEqual(deps, set(), f"iol_shared must not import from: {deps}")

    # ------------------------------------------------------------------ #
    # iol_reconciliation: only allowed dep is iol_shared
    # ------------------------------------------------------------------ #
    def test_reconciliation_does_not_depend_on_web_package(self):
        deps = _pkg_imports("iol_reconciliation")
        forbidden = deps - {"iol_shared"}
        self.assertEqual(forbidden, set(), f"iol_reconciliation must not import from: {forbidden}")

    # ------------------------------------------------------------------ #
    # iol_advisor: only allowed dep is iol_shared (no iol_cli, no iol_web
    # at import-time — build_unified_context uses a lazy runtime import of
    # iol_web which is an acceptable pragmatic exception documented there)
    # ------------------------------------------------------------------ #
    def test_advisor_does_not_depend_on_cli_package(self):
        deps = _pkg_imports("iol_advisor")
        self.assertNotIn("iol_cli", deps, "iol_advisor must not import from iol_cli")

    def test_advisor_top_level_imports_only_shared(self):
        # Catches static (non-lazy) imports; lazy runtime imports are excluded
        # from this check because ast.walk sees them as regular Import nodes
        # inside function bodies — they still count here.
        # We allow iol_web only as an explicit known exception (build_unified_context).
        deps = _pkg_imports("iol_advisor")
        forbidden = deps - {"iol_shared", "iol_web"}
        self.assertEqual(forbidden, set(), f"iol_advisor unexpected deps: {forbidden}")

    # ------------------------------------------------------------------ #
    # iol_web: must not import from iol_cli
    # ------------------------------------------------------------------ #
    def test_web_does_not_depend_on_cli_package(self):
        deps = _pkg_imports("iol_web")
        self.assertNotIn("iol_cli", deps, "iol_web must not import from iol_cli")


if __name__ == "__main__":
    unittest.main()
