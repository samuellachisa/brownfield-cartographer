from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, List, Literal, Optional, Tuple

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
class FunctionInfo:
    """Rich function/method info from AST."""
    name: str
    kind: Literal["function", "method"]
    decorators: List[str]
    owning_class: Optional[str] = None
    params_snippet: str = ""


@dataclass
class ClassInfo:
    """Rich class info from AST."""
    name: str
    superclasses: List[str]
    decorators: List[str]


@dataclass
class SQLStatementInfo:
    """Structural summary of a SQL statement from tree-sitter."""
    statement_type: str
    snippet: str
    line_start: int


@dataclass
class ModuleAnalysisResult:
    imports: List[str]
    public_functions: List[str]
    public_classes: List[str]
    loc: int
    comment_ratio: float
    cyclomatic_complexity: float = 0.0
    # Extended Python extraction
    functions: List[FunctionInfo] = field(default_factory=list)
    classes: List[ClassInfo] = field(default_factory=list)
    # SQL structural extraction (when tree-sitter SQL is used)
    sql_statements: List[SQLStatementInfo] = field(default_factory=list)


class TreeSitterAnalyzer:
    """
    Multi-language AST parsing with a LanguageRouter.

    Extracts: imports, functions/methods (with decorators), classes (with inheritance),
    SQL statement structure, and YAML keys. LOC/comment metrics computed from text.
    """

    def __init__(self, router: LanguageRouter | None = None) -> None:
        self.router = router or LanguageRouter.from_installed()

    def analyze_module(self, path: Path) -> ModuleAnalysisResult:
        lang, lang_name = self.router.for_path(path)
        parser = Parser(lang)

        source = path.read_text(encoding="utf-8", errors="ignore")
        try:
            tree = parser.parse(bytes(source, "utf8"))
        except Exception:
            lines = [ln for ln in source.splitlines()]
            loc = len([ln for ln in lines if ln.strip()])
            comment_lines = [ln for ln in lines if ln.strip().startswith("#")]
            comment_ratio = (len(comment_lines) / loc) if loc else 0.0
            return ModuleAnalysisResult(
                imports=[],
                public_functions=[],
                public_classes=[],
                loc=loc,
                comment_ratio=comment_ratio,
            )

        imports: List[str] = []
        public_functions: List[str] = []
        public_classes: List[str] = []
        functions: List[FunctionInfo] = []
        classes: List[ClassInfo] = []
        sql_statements: List[SQLStatementInfo] = []

        root = tree.root_node

        cyclomatic = 0.0
        if lang_name == "python":
            self._analyze_python(root, source, imports, public_functions, public_classes, functions, classes)
            cyclomatic = self._cyclomatic_python(root)
        elif lang_name == "sql":
            self._analyze_sql(root, source, imports, sql_statements)
        elif lang_name == "yaml":
            self._analyze_yaml(root, source, imports)

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
            cyclomatic_complexity=cyclomatic,
            functions=functions,
            classes=classes,
            sql_statements=sql_statements,
        )

    @staticmethod
    def _cyclomatic_python(root: Any) -> float:
        """Compute cyclomatic complexity from Python AST (1 + decision points)."""
        decision_types = (
            "if_statement",
            "elif_clause",
            "for_statement",
            "while_statement",
            "except_clause",
            "match_statement",
            "conditional_expression",
        )
        count = 0

        def walk(node: Any) -> None:
            nonlocal count
            if node.type in decision_types:
                count += 1
            for c in node.children:
                walk(c)

        walk(root)
        return float(max(1, 1 + count))

    def _analyze_python(
        self,
        root: Any,
        source: str,
        imports: List[str],
        public_functions: List[str],
        public_classes: List[str],
        functions: List[FunctionInfo],
        classes: List[ClassInfo],
    ) -> None:
        def get_text(n: Any) -> str:
            return source[n.start_byte : n.end_byte]

        def get_identifier(n: Any) -> Optional[str]:
            for c in n.children:
                if c.type == "identifier":
                    return get_text(c)
            return None

        def extract_superclasses(class_node: Any) -> List[str]:
            bases: List[str] = []
            for c in class_node.children:
                if c.type == "argument_list":
                    for arg in c.children:
                        if arg.type not in ("(", ")"):
                            bases.append(get_text(arg).strip())
                    break
            return bases

        def get_params_snippet(n: Any) -> str:
            for c in n.children:
                if c.type == "parameters":
                    return get_text(c)
            return ""

        def add_function(
            node: Any,
            decs: List[str],
            kind: Literal["function", "method"],
            owning_class: Optional[str] = None,
        ) -> None:
            name = get_identifier(node)
            if not name or name.startswith("_"):
                return
            params = get_params_snippet(node)
            functions.append(FunctionInfo(
                name=name,
                kind=kind,
                decorators=decs,
                owning_class=owning_class,
                params_snippet=params,
            ))
            public_functions.append(f"{name}{params}")

        def add_class(node: Any, decs: List[str]) -> Optional[str]:
            name = get_identifier(node)
            if not name or name.startswith("_"):
                return None
            superclasses = extract_superclasses(node)
            classes.append(ClassInfo(name=name, superclasses=superclasses, decorators=decs))
            label = f"{name}({','.join(superclasses)})" if superclasses else name
            public_classes.append(label)
            return name

        for node in root.children:
            if node.type in ("import_statement", "import_from_statement"):
                imports.append(get_text(node).strip())

            decs: List[str] = []
            if node.type == "decorated_definition":
                decs = [get_text(c).strip() for c in node.children if c.type == "decorator"]
                def_child = next((c for c in node.children if c.type in ("function_definition", "class_definition")), None)
                if not def_child:
                    continue
                node = def_child

            if node.type == "function_definition":
                add_function(node, decs, "function", None)
            elif node.type == "class_definition":
                class_name = add_class(node, decs)
                if class_name:
                    for c in node.children:
                        if c.type == "block":
                            for stmt in c.children:
                                m_decs: List[str] = []
                                m_def: Any = None
                                if stmt.type == "decorated_definition":
                                    m_decs = [get_text(d).strip() for d in stmt.children if d.type == "decorator"]
                                    m_def = next((d for d in stmt.children if d.type == "function_definition"), None)
                                elif stmt.type == "function_definition":
                                    m_def = stmt
                                if m_def:
                                    add_function(m_def, m_decs, "method", class_name)

    def _analyze_sql(
        self,
        root: Any,
        source: str,
        imports: List[str],
        sql_statements: List[SQLStatementInfo],
    ) -> None:
        stmt_types = ("create", "select", "insert", "update", "delete", "drop", "alter", "merge")
        for node in root.children:
            snippet = source[node.start_byte : node.end_byte].strip()
            if not snippet:
                continue
            first_line = snippet.splitlines()[0] if snippet else ""
            imports.append(first_line)
            # Infer statement type from node type or first token
            node_type = (node.type or "").lower()
            stmt_type = "unknown"
            for t in stmt_types:
                if t in node_type or first_line.strip().upper().startswith(t.upper()):
                    stmt_type = t
                    break
            line_start = source[: node.start_byte].count("\n") + 1
            sql_statements.append(SQLStatementInfo(statement_type=stmt_type, snippet=first_line[:200], line_start=line_start))

    def _analyze_yaml(self, root: Any, source: str, imports: List[str]) -> None:
        # tree-sitter-yaml (when installed): extract keys from block_mapping_pair, etc.
        # Fallback: line-based key extraction for robustness across grammars.
        seen: set[str] = set()

        def add_key(key: str) -> None:
            if key and key not in seen:
                seen.add(key)
                imports.append(key)

        for node in root.children:
            text = source[node.start_byte : node.end_byte]
            ntype = (node.type or "").lower()
            if "block_mapping_pair" in ntype or "pair" in ntype:
                for c in node.children:
                    ctext = source[c.start_byte : c.end_byte]
                    if ":" in ctext:
                        add_key(ctext.split(":")[0].strip())
            for line in text.splitlines():
                stripped = line.strip()
                if stripped and not stripped.startswith("#") and ":" in stripped:
                    add_key(stripped.split(":", 1)[0].strip())

