from pathlib import Path

from src.agents.hydrologist import HydrologistAgent, HydrologistConfig
from src.graph.knowledge_graph import KnowledgeGraph


def test_hydrologist_builds_unified_lineage(tmp_path: Path) -> None:
    # Python data operation
    py_code = '''
import pandas as pd

df = pd.read_sql_table("py_source", con=None)
df.to_sql("py_target", con=None)
'''
    (tmp_path / "script.py").write_text(py_code, encoding="utf-8")

    # SQL transformation
    sql_code = """
create table sql_target as
select * from py_target;
"""
    (tmp_path / "transform.sql").write_text(sql_code, encoding="utf-8")

    # YAML config (dbt-style)
    yaml_code = """
models:
  yaml_model:
    materialized: table
"""
    (tmp_path / "models.yaml").write_text(yaml_code, encoding="utf-8")

    kg = KnowledgeGraph()
    hydrologist = HydrologistAgent(HydrologistConfig(dialect="ansi"))
    datasets = hydrologist.run(tmp_path, kg, changed_files=None)

    # We expect datasets from Python, SQL, and YAML
    names = set(datasets.keys())
    assert "py_source" in names
    assert "py_target" in names
    assert "sql_target" in names
    assert "yaml_model" in names

    # Unified lineage graph should support sources/sinks API
    sources = set(kg.find_sources())
    sinks = set(kg.find_sinks())

    assert "py_source" in sources
    assert "sql_target" in sinks or "py_target" in sinks

