"""Tests for tree-sitter analyzer extended extraction: decorators, methods vs functions, inheritance, SQL structure."""

from pathlib import Path

from src.analyzers.tree_sitter_analyzer import (
    ClassInfo,
    FunctionInfo,
    ModuleAnalysisResult,
    SQLStatementInfo,
    TreeSitterAnalyzer,
)


def test_python_function_vs_method_distinction(tmp_path: Path) -> None:
    py = tmp_path / "mod.py"
    py.write_text("""
def top_level_func() -> None:
    pass

class MyClass:
    def instance_method(self, x: int) -> str:
        return str(x)

    @staticmethod
    def static_meth() -> bool:
        return True
""")
    analyzer = TreeSitterAnalyzer()
    result = analyzer.analyze_module(py)
    assert isinstance(result, ModuleAnalysisResult)
    funcs = [f for f in result.functions if f.name == "top_level_func"]
    assert len(funcs) == 1
    assert funcs[0].kind == "function"
    assert funcs[0].owning_class is None

    methods = [f for f in result.functions if f.owning_class == "MyClass"]
    assert len(methods) >= 2
    names = {m.name for m in methods}
    assert "instance_method" in names
    assert "static_meth" in names
    for m in methods:
        assert m.kind == "method"


def test_python_inheritance_extraction(tmp_path: Path) -> None:
    py = tmp_path / "mod.py"
    py.write_text("""
class BaseService:
    pass

class DerivedService(BaseService):
    pass

class MultiBases(A, B, C):
    pass
""")
    analyzer = TreeSitterAnalyzer()
    result = analyzer.analyze_module(py)
    derived = next((c for c in result.classes if c.name == "DerivedService"), None)
    assert derived is not None
    assert "BaseService" in derived.superclasses

    multi = next((c for c in result.classes if c.name == "MultiBases"), None)
    assert multi is not None
    assert len(multi.superclasses) >= 2


def test_python_decorators_extraction(tmp_path: Path) -> None:
    py = tmp_path / "mod.py"
    py.write_text("""
@dataclass
class DataModel:
    x: int

@property
def computed() -> int:
    return 42

@app.route("/api")
def api_handler() -> dict:
    return {}
""")
    analyzer = TreeSitterAnalyzer()
    result = analyzer.analyze_module(py)
    dataclass_cls = next((c for c in result.classes if c.name == "DataModel"), None)
    assert dataclass_cls is not None
    assert any("dataclass" in d for d in dataclass_cls.decorators)

    api_func = next((f for f in result.functions if f.name == "api_handler"), None)
    assert api_func is not None
    assert any("route" in d for d in api_func.decorators)


def test_sql_statement_structure(tmp_path: Path) -> None:
    sql = tmp_path / "schema.sql"
    sql.write_text("""
CREATE TABLE users (id INT, name TEXT);
SELECT * FROM users WHERE id = 1;
INSERT INTO users (id, name) VALUES (1, 'a');
""")
    analyzer = TreeSitterAnalyzer()
    result = analyzer.analyze_module(sql)
    assert len(result.sql_statements) >= 1
    types = {s.statement_type for s in result.sql_statements}
    assert "create" in types or "select" in types or "insert" in types or "unknown" in types


def test_yaml_key_extraction(tmp_path: Path) -> None:
    yaml = tmp_path / "config.yaml"
    yaml.write_text("""
models:
  - name: my_model
sources:
  raw: ./raw
schedule: daily
""")
    analyzer = TreeSitterAnalyzer()
    result = analyzer.analyze_module(yaml)
    assert len(result.imports) >= 1
    keys = set(result.imports)
    assert "models" in keys or "sources" in keys or "schedule" in keys


def test_backward_compatible_public_functions_and_classes(tmp_path: Path) -> None:
    py = tmp_path / "mod.py"
    py.write_text("""
def foo(x: int) -> str:
    pass

class Bar(Base):
    def baz(self) -> None:
        pass
""")
    analyzer = TreeSitterAnalyzer()
    result = analyzer.analyze_module(py)
    assert "foo(" in result.public_functions[0] or "foo" in str(result.public_functions)
    assert "Bar" in str(result.public_classes)
    assert result.loc > 0
