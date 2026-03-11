from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Tuple, Literal, Optional

from tree_sitter import Language, Parser


@dataclass
class LanguageRouter:
    """
    Selects the correct tree-sitter language based on file extension.

    This implementation assumes the presence of tree-sitter-python and
    tree-sitter-sql dynamic libraries via pip packages and will attempt
    to use tree-sitter-yaml when available.
    """

    python: Language
    sql: Language
    yaml: Optional[Language] = None

    @classmethod
    def from_installed(cls) -> "LanguageRouter":
        # Prebuilt packages expose a PyCapsule; wrap with tree_sitter.Language for 0.25+ API.
        from tree_sitter_python import language as ts_python  # type: ignore
        from tree_sitter_sql import language as ts_sql  # type: ignore
        yaml_lang: Optional[Language] = None
        try:
            # tree_sitter_yaml is optional; if it is not installed we still
            # support Python/SQL analysis and fall back to non-AST YAML parsing.
            from tree_sitter_yaml import language as ts_yaml  # type: ignore

            yaml_lang = Language(ts_yaml())
        except Exception:
            yaml_lang = None

        return cls(python=Language(ts_python()), sql=Language(ts_sql()), yaml=yaml_lang)

    def for_path(self, path: Path) -> Tuple[Language, Literal["python", "sql", "yaml", "other"]]:
        suffix = path.suffix.lower()
        if suffix == ".py":
            return self.python, "python"
        if suffix == ".sql":
            return self.sql, "sql"
        if suffix in {".yaml", ".yml"}:
            if self.yaml is not None:
                return self.yaml, "yaml"
            # Fallback: reuse Python grammar but keep the yaml tag so callers
            # can still distinguish configuration files.
            return self.python, "yaml"
        return self.python, "other"


@dataclass
class ModuleAnalysisResult:
    imports: List[str]
    public_functions: List[str]
    public_classes: List[str]
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
        parser = Parser(lang)

        source = path.read_text(encoding="utf-8", errors="ignore")
        tree = parser.parse(bytes(source, "utf8"))

        imports: List[str] = []
        public_functions: List[str] = []
        public_classes: List[str] = []

        root = tree.root_node

        if lang_name == "python":
            for node in root.children:
                if node.type in ("import_statement", "import_from_statement"):
                    imports.append(source[node.start_byte : node.end_byte].strip())
                if node.type == "function_definition":
                    name_node = next((c for c in node.children if c.type == "identifier"), None)
                    if name_node is not None:
                        name = source[name_node.start_byte : name_node.end_byte]
                        if not name.startswith("_"):
                            public_functions.append(name)
                if node.type == "class_definition":
                    name_node = next((c for c in node.children if c.type == "identifier"), None)
                    if name_node is not None:
                        name = source[name_node.start_byte : name_node.end_byte]
                        if not name.startswith("_"):
                            public_classes.append(name)
        elif lang_name == "sql":
            # For SQL we rely on sqlglot for rich structure, but we still
            # capture a lightweight structural summary of statements.
            for node in root.children:
                snippet = source[node.start_byte : node.end_byte].strip()
                if snippet:
                    imports.append(snippet.splitlines()[0])
        elif lang_name == "yaml":
            # Capture top-level keys in YAML configs as structural elements.
            for node in root.children:
                text = source[node.start_byte : node.end_byte]
                for line in text.splitlines():
                    stripped = line.strip()
                    if stripped and not stripped.startswith("#") and ":" in stripped:
                        key = stripped.split(":", 1)[0].strip()
                        if key and key not in imports:
                            imports.append(key)

        lines = [ln for ln in source.splitlines()]
        loc = len([ln for ln in lines if ln.strip()])
        comment_lines = [ln for ln in lines if ln.strip().startswith("#")]
        comment_ratio = (len(comment_lines) / loc) if loc else 0.0

        return ModuleAnalysisResult(
            imports=imports,
            public_functions=public_functions,
            public_classes=public_classes,
            loc=loc,
            comment_ratio=comment_ratio,
        )

