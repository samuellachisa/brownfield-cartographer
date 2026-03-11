from pathlib import Path

from src.analyzers.python_lineage import PythonLineageAnalyzer


def test_python_lineage_detects_writes_and_reads(tmp_path: Path) -> None:
    code = """
import pandas as pd

df = pd.read_sql_table("source_table", con=None)
df.to_sql("target_table", con=None)

spark.read.table("spark_source")
"""
    path = tmp_path / "example.py"
    path.write_text(code, encoding="utf-8")

    analyzer = PythonLineageAnalyzer()
    deps = list(analyzer.analyze_file(path))

    assert deps, "Expected at least one detected data dependency"

    sources = {s for d in deps for s in d.sources}
    targets = {t for d in deps for t in d.targets}

    assert "source_table" in sources
    assert "spark_source" in sources
    assert "target_table" in targets

