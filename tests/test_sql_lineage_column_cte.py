"""Tests for column-level lineage and nested CTE/subquery enhancements."""

from pathlib import Path

from src.analyzers.sql_lineage import (
    ColumnLineageEdge,
    SQLLineageAnalyzer,
)


def test_column_lineage_insert_select(tmp_path: Path) -> None:
    sql = """
    INSERT INTO analytics.target (id, name, total)
    SELECT user_id, username, amount
    FROM raw.users;
    """
    path = tmp_path / "insert_select.sql"
    path.write_text(sql)
    analyzer = SQLLineageAnalyzer()
    deps = list(analyzer.analyze_file(path))
    assert deps, "Expected at least one dependency"
    dep = deps[0]

    assert any("target" in t for t in dep.targets)
    assert dep.write_columns, "Expected write_columns"
    tgt_key = next((k for k in dep.write_columns if "target" in k), None)
    assert tgt_key
    assert set(dep.write_columns[tgt_key]) >= {"id", "name", "total"}

    assert dep.column_lineage, "Expected column lineage"
    edges = {(e.source_table, e.source_column, e.target_table, e.target_column) for e in dep.column_lineage}
    assert any(
        "users" in st and "user_id" in sc and "target" in tt and "id" in tc
        for (st, sc, tt, tc) in edges
    )


def test_cte_resolved_nested(tmp_path: Path) -> None:
    sql = """
    WITH
        level1 AS (SELECT * FROM raw.events),
        level2 AS (SELECT * FROM level1),
        level3 AS (SELECT * FROM level2 JOIN raw.users ON 1)
    SELECT * FROM level3;
    """
    path = tmp_path / "nested_cte.sql"
    path.write_text(sql)
    analyzer = SQLLineageAnalyzer()
    deps = list(analyzer.analyze_file(path))
    assert deps
    dep = deps[0]

    assert dep.cte_dependencies
    assert "level1" in dep.cte_dependencies
    assert "level2" in dep.cte_dependencies
    assert "level3" in dep.cte_dependencies

    assert dep.cte_resolved
    # level1 -> raw.events; level2 -> level1 -> raw.events; level3 -> level2, raw.users
    assert "raw.events" in dep.cte_resolved.get("level1", [])
    assert "raw.events" in dep.cte_resolved.get("level2", [])
    assert "raw.events" in dep.cte_resolved.get("level3", [])
    assert "raw.users" in dep.cte_resolved.get("level3", [])


def test_subquery_dependencies(tmp_path: Path) -> None:
    sql = """
    SELECT *
    FROM (
        SELECT a, b FROM base_table
    ) AS derived
    JOIN other_table ON derived.a = other_table.x;
    """
    path = tmp_path / "subquery.sql"
    path.write_text(sql)
    analyzer = SQLLineageAnalyzer()
    deps = list(analyzer.analyze_file(path))
    assert deps
    dep = deps[0]

    assert dep.subquery_dependencies
    assert "derived" in dep.subquery_dependencies
    assert "base_table" in dep.subquery_dependencies["derived"]
