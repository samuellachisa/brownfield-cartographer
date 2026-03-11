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

    # ansi, bigquery and snowflake should all parse this simple statement
    for dialect in ("ansi", "bigquery", "snowflake"):
        analyzer = SQLLineageAnalyzer(dialect=dialect)
        deps = list(analyzer.analyze_file(path))
        assert deps, f"Expected dependencies for dialect={dialect}"
        dep = deps[0]
        assert "t1" in dep.targets


def test_sql_lineage_excludes_cte_names_from_sources(tmp_path: Path) -> None:
    sql = """
    with recent_orders as (
        select * from raw.orders
    )
    create table analytics.top_orders as
    select * from recent_orders;
    """
    path = tmp_path / "cte.sql"
    path.write_text(sql, encoding="utf-8")

    analyzer = SQLLineageAnalyzer(dialect="ansi")
    deps = list(analyzer.analyze_file(path))
    # We expect at least one dependency where the physical source table is
    # raw.orders and the target is analytics.top_orders, without treating the
    # CTE name as its own dataset.
    combined_sources = {s for d in deps for s in d.sources}
    combined_targets = {t for d in deps for t in d.targets}

    assert "raw.orders" in combined_sources
    assert "recent_orders" not in combined_sources
    assert any("analytics.top_orders" in t for t in combined_targets)


def test_sql_lineage_tracks_read_columns(tmp_path: Path) -> None:
    sql = """
    select o.id, o.total, u.email
    from raw.orders o
    join raw.users u on o.user_id = u.id;
    """
    path = tmp_path / "cols.sql"
    path.write_text(sql, encoding="utf-8")

    analyzer = SQLLineageAnalyzer(dialect="ansi")
    deps = list(analyzer.analyze_file(path))
    assert deps, "Expected at least one SQL dependency"
    dep = deps[0]

    # Column lineage is best-effort, but we should at least see some columns
    # recorded for one of the source tables, or the generic '__unresolved__'
    # bucket.
    assert dep.read_columns, "Expected read_columns to be populated"
    all_cols = {c for cols in dep.read_columns.values() for c in cols}
    assert "id" in all_cols or "total" in all_cols or "email" in all_cols
