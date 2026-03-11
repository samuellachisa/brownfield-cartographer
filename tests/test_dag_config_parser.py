from pathlib import Path

from src.analyzers.dag_config_parser import DAGConfigParser


def test_airflow_yaml_parses_tasks_and_dependencies(tmp_path: Path) -> None:
    text = """
    dags:
      example_dag:
        schedule_interval: "0 12 * * *"
    tasks:
      extract:
        upstream: []
      transform:
        upstream:
          - extract
      load:
        upstream:
          - transform
    """
    path = tmp_path / "dag.yml"
    path.write_text(text, encoding="utf-8")

    parser = DAGConfigParser()
    cfg = parser.parse(path)
    assert cfg is not None
    assert cfg.config_type == "airflow"
    assert set(cfg.tasks) == {"extract", "transform", "load"}
    # Task-level dependencies
    assert cfg.dependencies["transform"] == ["extract"]
    assert cfg.dependencies["load"] == ["transform"]
    # Schedule is propagated from the top-level
    assert cfg.schedule == "0 12 * * *"


def test_unknown_yaml_returns_minimal_config(tmp_path: Path) -> None:
    text = """
    some_key: value
    """
    path = tmp_path / "config.yml"
    path.write_text(text, encoding="utf-8")

    parser = DAGConfigParser()
    cfg = parser.parse(path)
    assert cfg is not None
    assert cfg.config_type == "unknown"
    assert cfg.tasks == []
    assert cfg.dependencies == {}
