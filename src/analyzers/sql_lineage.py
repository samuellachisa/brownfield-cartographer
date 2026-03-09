from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Tuple

import sqlglot


@dataclass
class SQLDependency:
    sources: List[str]
    targets: List[str]
    sql: str


class SQLLineageAnalyzer:
    """
    sqlglot-based SQL dependency extraction.

    Given a .sql file, extracts table dependencies from SELECT/FROM/JOIN/CTE
    chains and DML/DDL targets.
    """

    def __init__(self, dialect: str | None = None) -> None:
        self.dialect = dialect

    def analyze_file(self, path: Path) -> Iterable[SQLDependency]:
        text = path.read_text(encoding="utf-8", errors="ignore")
        statements = sqlglot.parse(text, read=self.dialect)
        for stmt in statements:
            sources = {t.sql(dialect=self.dialect) for t in stmt.find_all("Table")}
            targets = set()
            for create in stmt.find_all("Create"):
                for t in create.find_all("Table"):
                    targets.add(t.sql(dialect=self.dialect))
            for insert in stmt.find_all("Insert"):
                if insert.this:
                    targets.add(insert.this.sql(dialect=self.dialect))
            yield SQLDependency(
                sources=sorted(sources),
                targets=sorted(targets),
                sql=stmt.sql(dialect=self.dialect),
            )

