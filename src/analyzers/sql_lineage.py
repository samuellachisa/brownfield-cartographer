from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Tuple, Optional, Set

import sqlglot
from sqlglot import exp


@dataclass
class SQLDependency:
    sources: List[str]
    targets: List[str]
    sql: str
    statement_index: int
    line_range: Optional[Tuple[int, int]]
    cte_names: List[str]
    # Optional column-level lineage: mapping table name -> list of columns
    read_columns: Dict[str, List[str]]
    # Best-effort join topology: each entry is a (table, join_type) pair for
    # tables appearing on the right-hand side of an explicit JOIN clause.
    joins: List[Tuple[str, str]]
    # CTE dependency graph: mapping CTE name -> list of physical source tables
    # referenced inside the CTE body (excluding other CTEs). This helps
    # reconstruct multi-hop lineage within complex WITH chains.
    cte_dependencies: Dict[str, List[str]]


class SQLLineageAnalyzer:
    """
    sqlglot-based SQL dependency extraction.

    Given a .sql file, extracts table dependencies from SELECT/FROM/JOIN/CTE
    chains and DML/DDL targets.
    """

    def __init__(self, dialect: str | None = None) -> None:
        self.dialect = dialect

    def analyze_file(self, path: Path) -> Iterable[SQLDependency]:
        """
        Parse a SQL file and return a sequence of SQLDependency objects.

        Design goals:
        - **Multi-dialect**: the `dialect` field is passed straight through to
          sqlglot so that callers can analyse BigQuery/Snowflake/DuckDB
          projects by selecting the appropriate dialect.
        - **Read vs write separation**: we treat **targets** as objects created
          or written to (CREATE/INSERT), and **sources** as tables read from in
          SELECT/FROM/JOIN clauses, excluding CTE names.
        - **CTE awareness**: we record the names of common table expressions
          and avoid counting them as physical source tables. This yields a
          cleaner lineage graph where edges connect real datasets rather than
          intra-query aliases.
        """
        text = path.read_text(encoding="utf-8", errors="ignore")

        # Best-effort normalization for common dbt-style ref() macros embedded
        # in templating (e.g. {{ ref("my_table") }}). We strip the templating
        # delimiters so sqlglot can parse the underlying SQL and separately
        # track the referenced objects as sources.
        ref_sources: set[str] = set()
        try:
            import re

            def _ref_repl(match: re.Match) -> str:
                inner = match.group(1)
                # inner is like ref('my_table') or ref(\"my_table\")
                name_match = re.search(r"ref\(['\"]([^'\"]+)['\"]\)", inner)
                if name_match:
                    tbl = name_match.group(1)
                    ref_sources.add(tbl)
                    return tbl
                return inner

            text_for_parse = re.sub(r"\{\{\s*(.*?)\s*\}\}", _ref_repl, text)
        except Exception:
            text_for_parse = text

        try:
            statements = sqlglot.parse(text_for_parse, read=self.dialect)
        except Exception as e:
            # Best-effort: log and skip files we can't tokenize/parse instead of
            # failing the entire hydrologist run.
            print(f"[hydrologist] Failed to parse SQL file {path} with dialect={self.dialect}: {e}")
            return []

        # Pre-split original text for best-effort line range calculation.
        lines = text.splitlines()
        joined = "\n".join(lines)

        for idx, stmt in enumerate(statements):
            # Collect CTE names up-front so we can exclude them from physical
            # source tables. This keeps the lineage graph focused on real
            # datasets.
            cte_names: List[str] = []
            for cte in stmt.find_all(exp.CTE):
                alias = None
                if isinstance(cte.this, exp.Alias):
                    alias = cte.this.alias
                elif isinstance(cte.this, exp.Table):
                    alias = cte.this.alias
                if alias and alias not in cte_names:
                    cte_names.append(alias)

            # Within each CTE, also record which physical tables it depends on.
            cte_dependencies: Dict[str, Set[str]] = {}
            for cte in stmt.find_all(exp.CTE):
                alias = None
                if isinstance(cte.this, exp.Alias):
                    alias = cte.this.alias
                elif isinstance(cte.this, exp.Table):
                    alias = cte.this.alias
                if not alias:
                    continue
                body = cte.this
                deps: Set[str] = set()
                for t in body.find_all(exp.Table):
                    if t.name not in cte_names:
                        deps.add(t.sql(dialect=self.dialect))
                if deps:
                    cte_dependencies[alias] = deps

            # Reads: every table reference in the statement that is not a CTE.
            sources = {
                t.sql(dialect=self.dialect)
                for t in stmt.find_all(exp.Table)
                if t.name not in cte_names
            }
            sources.update(ref_sources)

            # Column-level read lineage: table -> set(columns)
            read_cols: Dict[str, Set[str]] = {}
            for col in stmt.find_all(exp.Column):
                # Best effort: prefer fully qualified `table.column`, but fall
                # back to just the column name if no table qualifier is present.
                tbl = None
                if col.table:
                    tbl = col.table
                # If we still don't know the table, attribute to a generic
                # bucket so callers can see that the column exists even if
                # we can't precisely resolve its table.
                if not tbl:
                    tbl = "__unresolved__"
                name = col.name or "*"
                if tbl not in read_cols:
                    read_cols[tbl] = set()
                read_cols[tbl].add(name)

            # Writes: CREATE/INSERT/UPDATE/DELETE targets.
            targets = set()
            for create in stmt.find_all(exp.Create):
                for t in create.find_all(exp.Table):
                    targets.add(t.sql(dialect=self.dialect))
            for insert in stmt.find_all(exp.Insert):
                if insert.this:
                    targets.add(insert.this.sql(dialect=self.dialect))
            for update in stmt.find_all(exp.Update):
                if update.this:
                    targets.add(update.this.sql(dialect=self.dialect))
            for delete in stmt.find_all(exp.Delete):
                if delete.this:
                    targets.add(delete.this.sql(dialect=self.dialect))

            # Join topology: for each JOIN, record the right-hand table and
            # join type (INNER/LEFT/RIGHT/FULL/CROSS...). We intentionally keep
            # this lightweight and do not attempt full alias resolution here.
            joins: List[Tuple[str, str]] = []
            for join in stmt.find_all(exp.Join):
                try:
                    right_expr = join.this
                    right_sql = right_expr.sql(dialect=self.dialect)
                    kind = join.args.get("kind") or "join"
                    join_type = str(kind).upper()
                    joins.append((right_sql, join_type))
                except Exception:
                    # Best-effort: skip joins we cannot render.
                    continue

            # Best-effort line range: search for the rendered SQL within the
            # original file text and compute 1-based line numbers.
            rendered = stmt.sql(dialect=self.dialect)
            start_line: Optional[int] = None
            end_line: Optional[int] = None
            try:
                offset = joined.index(rendered)
            except ValueError:
                offset = -1
            if offset >= 0:
                start_line = joined[:offset].count("\n") + 1
                end_line = start_line + rendered.count("\n")

            yield SQLDependency(
                sources=sorted(sources),
                targets=sorted(targets),
                sql=rendered,
                statement_index=idx,
                line_range=(start_line, end_line) if start_line is not None and end_line is not None else None,
                cte_names=cte_names,
                read_columns={t: sorted(cols) for t, cols in read_cols.items()},
                joins=joins,
                cte_dependencies={name: sorted(deps) for name, deps in cte_dependencies.items()},
            )

