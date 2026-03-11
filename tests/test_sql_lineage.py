from pathlib import Path

from src.analyzers.sql_lineage import SQLLineageAnalyzer


def test_sql_lineage_handles_dbt_ref(tmp_path: Path) -> None:
    sql = """
    create table analytics.target_table as
    select * from {{ ref("source_table") }};
    """
    path = tmp_path / "model.sql"
    path.write_text(sql, encoding="utf-8")

    analyzer = SQLLineageAnalyzer(dialect="ansi")
    deps = list(analyzer.analyze_file(path))

    assert deps, "Expected at least one SQL dependency"
    dep = deps[0]

    assert "source_table" in dep.sources
    assert "analytics.target_table" in dep.targets or "target_table" in dep.targets
    # Enriched metadata
    assert dep.statement_index == 0
    assert dep.line_range is not None
    start, end = dep.line_range
    assert 1 <= start <= end


def test_sql_lineage_supports_multiple_dialects(tmp_path: Path) -> None:
    sql = "CREATE TABLE t1 AS SELECT 1 AS x;"
    path = tmp_path / "ddl.sql"
    path.write_text(sql, encoding="utf-8")

    # ansi and bigquery should both parse this simple statement
    for dialect in ("ansi", "bigquery"):
        analyzer = SQLLineageAnalyzer(dialect=dialect)
        deps = list(analyzer.analyze_file(path))
        assert deps, f"Expected dependencies for dialect={dialect}"
        dep = deps[0]
        assert "t1" in dep.targets
