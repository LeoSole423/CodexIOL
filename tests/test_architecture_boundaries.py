import ast
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1] / "src"


def _imports_for(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for item in node.names:
                names.add(item.name)
        elif isinstance(node, ast.ImportFrom) and node.module:
            names.add(node.module)
    return names


class TestArchitectureBoundaries(unittest.TestCase):
    def test_reconciliation_does_not_depend_on_web_package(self):
        imports = _imports_for(ROOT / "iol_reconciliation" / "service.py")
        self.assertFalse(any(name == "iol_web" or name.startswith("iol_web.") for name in imports))


if __name__ == "__main__":
    unittest.main()
