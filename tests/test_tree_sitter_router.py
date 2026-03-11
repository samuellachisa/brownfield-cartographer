from pathlib import Path

from src.analyzers.tree_sitter_analyzer import LanguageRouter


def test_language_router_routes_extensions(tmp_path: Path) -> None:
    router = LanguageRouter.from_installed()

    py_path = tmp_path / "file.py"
    sql_path = tmp_path / "file.sql"
    yaml_path = tmp_path / "file.yaml"

    lang, name = router.for_path(py_path)
    assert name == "python"

    lang, name = router.for_path(sql_path)
    assert name == "sql"

    lang, name = router.for_path(yaml_path)
    assert name == "yaml"

