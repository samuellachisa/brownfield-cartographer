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

