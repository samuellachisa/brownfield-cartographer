from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Tuple, Literal

from tree_sitter import Language, Parser


@dataclass
class LanguageRouter:
    """
    Selects the correct tree-sitter language based on file extension.

    This implementation assumes the presence of tree-sitter-python and
    tree-sitter-sql dynamic libraries via pip packages.
    """

    python: Language
    sql: Language

    @classmethod
    def from_installed(cls) -> "LanguageRouter":
        # The Python bindings expose prebuilt Language objects via these modules.
        from tree_sitter_python import language as ts_python  # type: ignore
        from tree_sitter_sql import language as ts_sql  # type: ignore

        return cls(python=ts_python(), sql=ts_sql())

    def for_path(self, path: Path) -> Tuple[Language, Literal["python", "sql", "yaml", "other"]]:
        suffix = path.suffix.lower()
        if suffix == ".py":
            return self.python, "python"
        if suffix == ".sql":
            return self.sql, "sql"
        # YAML and notebooks are not handled via tree-sitter here
        return self.python, "other"


@dataclass
class ModuleAnalysisResult:
    imports: List[str]
    public_functions: List[str]
    loc: int
    comment_ratio: float


class TreeSitterAnalyzer:
    """
    Multi-language AST parsing with a LanguageRouter.

    For this implementation, we use tree-sitter for Python/SQL to extract:
    - imports
    - public function names
    - basic LOC/comment metrics (computed from text)
    """

    def __init__(self, router: LanguageRouter | None = None) -> None:
        self.router = router or LanguageRouter.from_installed()

    def analyze_module(self, path: Path) -> ModuleAnalysisResult:
        lang, lang_name = self.router.for_path(path)
        parser = Parser()
        parser.set_language(lang)

        source = path.read_text(encoding="utf-8", errors="ignore")
        tree = parser.parse(bytes(source, "utf8"))

        imports: List[str] = []
        public_functions: List[str] = []

        if lang_name == "python":
            root = tree.root_node
            for node in root.children:
                if node.type in ("import_statement", "import_from_statement"):
                    # crude text slice
                    imports.append(source[node.start_byte : node.end_byte].strip())
                if node.type == "function_definition":
                    name_node = next((c for c in node.children if c.type == "identifier"), None)
                    if name_node is not None:
                        name = source[name_node.start_byte : name_node.end_byte]
                        if not name.startswith("_"):
                            public_functions.append(name)

        lines = [ln for ln in source.splitlines()]
        loc = len([ln for ln in lines if ln.strip()])
        comment_lines = [ln for ln in lines if ln.strip().startswith("#")]
        comment_ratio = (len(comment_lines) / loc) if loc else 0.0

        return ModuleAnalysisResult(
            imports=imports,
            public_functions=public_functions,
            loc=loc,
            comment_ratio=comment_ratio,
        )

