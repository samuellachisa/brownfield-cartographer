from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Tuple, Optional, Set

import sqlglot
from sqlglot import exp


@dataclass
class ColumnLineageEdge:
    """Single column-level lineage edge: source (table, col) -> target (table, col)."""

    source_table: str
    source_column: str
    target_table: str
    target_column: str


@dataclass
class SQLDependency:
    sources: List[str]
    targets: List[str]
    sql: str
    statement_index: int
    line_range: Optional[Tuple[int, int]]
    cte_names: List[str]
    # Column-level read lineage: table -> list of columns read
    read_columns: Dict[str, List[str]]
    # Column-level write lineage: table -> list of columns written
    write_columns: Dict[str, List[str]]
    # Column lineage: explicit source (table,col) -> target (table,col) mappings
    column_lineage: List[ColumnLineageEdge]
    # Best-effort join topology
    joins: List[Tuple[str, str]]
    # CTE -> direct dependencies (tables or other CTEs)
    cte_dependencies: Dict[str, List[str]]
    # CTE -> all physical tables (transitive resolution for nested CTEs)
    cte_resolved: Dict[str, List[str]]
    # Subquery/derived table alias -> source tables
    subquery_dependencies: Dict[str, List[str]]


class SQLLineageAnalyzer:
    """
    sqlglot-based SQL dependency extraction.

    Given a .sql file, extracts table dependencies from SELECT/FROM/JOIN/CTE
    chains and DML/DDL targets.
    """

    def __init__(self, dialect: str | None = None) -> None:
        self.dialect = dialect

    @staticmethod
    def _resolve_cte_to_physical(
        cte_names: Set[str],
        cte_dependencies: Dict[str, Set[str]],
    ) -> Dict[str, List[str]]:
        """Resolve each CTE to all physical tables (transitive)."""
        physical = {t for t in cte_names if t not in cte_dependencies}
        resolved: Dict[str, List[str]] = {}

        def _resolve(name: str) -> Set[str]:
            if name in resolved:
                return set(resolved[name])
            deps = cte_dependencies.get(name, set())
            out: Set[str] = set()
            for d in deps:
                if d in cte_dependencies:
                    out |= _resolve(d)
                else:
                    out.add(d)
            resolved[name] = sorted(out)
            return out

        for name in cte_names:
            if name in cte_dependencies:
                _resolve(name)
        return resolved

    def _extract_column_lineage(
        self,
        stmt: exp.Expression,
        cte_names: List[str],
    ) -> Tuple[Dict[str, List[str]], List[ColumnLineageEdge]]:
        """Extract write_columns and column_lineage for INSERT...SELECT, CREATE, MERGE."""
        write_cols: Dict[str, Set[str]] = {}
        col_lineage: List[ColumnLineageEdge] = []

        for insert in stmt.find_all(exp.Insert):
            schema = insert.this
            tgt_table = ""
            if isinstance(schema, exp.Schema):
                tbl_node = schema.this
                tgt_table = tbl_node.sql(dialect=self.dialect) if tbl_node else ""
                tgt_cols = [
                    (e.sql(dialect=self.dialect) if hasattr(e, "sql") else str(e))
                    for e in getattr(schema, "expressions", []) or []
                ]
            else:
                tgt_table = schema.sql(dialect=self.dialect) if schema else ""
                tgt_cols = []
            if not tgt_table or tgt_table in cte_names:
                continue
            if tgt_cols:
                write_cols.setdefault(tgt_table, set()).update(tgt_cols)

            expr = insert.expression
            if expr and isinstance(expr, exp.Select):
                src_cols = expr.named_selects or [e.sql(dialect=self.dialect) for e in (expr.expressions or [])]
                src_tables = [t.sql(dialect=self.dialect) for t in expr.find_all(exp.Table) if t.name and t.name not in cte_names]
                src_table = src_tables[0] if src_tables else "__select__"
                for i, tcol in enumerate(tgt_cols):
                    if i < len(src_cols):
                        col_lineage.append(
                            ColumnLineageEdge(
                                source_table=src_table,
                                source_column=src_cols[i],
                                target_table=tgt_table,
                                target_column=tcol,
                            )
                        )

        for create in stmt.find_all(exp.Create):
            for t in create.find_all(exp.Table):
                tbl = t.sql(dialect=self.dialect)
                if not tbl or tbl in cte_names:
                    continue
                write_cols.setdefault(tbl, set())
                for col in create.find_all(exp.ColumnDef):
                    cname = getattr(col, "name", None) or (col.this.sql(dialect=self.dialect) if col.this else None)
                    if cname:
                        write_cols[tbl].add(cname)

        return (
            {t: sorted(c) for t, c in write_cols.items()},
            col_lineage,
        )

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
            # source tables.
            cte_names: List[str] = []
            for cte in stmt.find_all(exp.CTE):
                alias = (cte.alias or "") if hasattr(cte, "alias") else ""
                if isinstance(alias, exp.Expression):
                    alias = alias.sql(dialect=self.dialect) if alias else ""
                if not alias:
                    alias = getattr(cte.this, "alias", None) or ""
                    if isinstance(alias, exp.Expression):
                        alias = alias.sql(dialect=self.dialect) if alias else ""
                if alias and alias not in cte_names:
                    cte_names.append(alias)

            # CTE -> direct dependencies (physical tables and other CTEs for nested resolution)
            cte_dependencies: Dict[str, Set[str]] = {}
            for cte in stmt.find_all(exp.CTE):
                alias = (cte.alias or "") if hasattr(cte, "alias") else ""
                if isinstance(alias, exp.Expression):
                    alias = alias.sql(dialect=self.dialect) if alias else ""
                if not alias:
                    alias = getattr(cte.this, "alias", None) or ""
                    if isinstance(alias, exp.Expression):
                        alias = alias.sql(dialect=self.dialect) if alias else ""
                if not alias:
                    continue
                body = cte.this
                deps: Set[str] = set()
                for t in body.find_all(exp.Table):
                    tbl_sql = t.sql(dialect=self.dialect)
                    if tbl_sql and tbl_sql != alias:
                        deps.add(tbl_sql)
                if deps:
                    cte_dependencies[alias] = deps

            # Resolve each CTE to all physical tables (transitive for nested CTEs)
            cte_resolved = self._resolve_cte_to_physical(set(cte_names), cte_dependencies)

            # Subquery/derived table alias -> source tables
            subquery_deps: Dict[str, Set[str]] = {}
            for subq in stmt.find_all(exp.Subquery):
                alias = subq.alias or ""
                if isinstance(alias, exp.Expression):
                    alias = alias.sql(dialect=self.dialect) if alias else ""
                if not alias:
                    continue
                sd: Set[str] = set()
                for t in subq.find_all(exp.Table):
                    tbl = t.sql(dialect=self.dialect)
                    if tbl and tbl != alias:
                        sd.add(tbl)
                if sd:
                    subquery_deps[alias] = sd

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
                    schema = insert.this
                    tbl = schema.this if isinstance(schema, exp.Schema) else schema
                    targets.add(tbl.sql(dialect=self.dialect))
            for update in stmt.find_all(exp.Update):
                if update.this:
                    targets.add(update.this.sql(dialect=self.dialect))
            for delete in stmt.find_all(exp.Delete):
                if delete.this:
                    targets.add(delete.this.sql(dialect=self.dialect))

            # Join topology
            joins: List[Tuple[str, str]] = []
            for join in stmt.find_all(exp.Join):
                try:
                    right_expr = join.this
                    right_sql = right_expr.sql(dialect=self.dialect)
                    kind = join.args.get("kind") or "join"
                    join_type = str(kind).upper()
                    joins.append((right_sql, join_type))
                except Exception:
                    continue

            # Column-level write and lineage
            write_columns, column_lineage = self._extract_column_lineage(stmt, cte_names)

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
                write_columns=write_columns,
                column_lineage=column_lineage,
                joins=joins,
                cte_dependencies={name: sorted(deps) for name, deps in cte_dependencies.items()},
                cte_resolved={name: deps for name, deps in cte_resolved.items()},
                subquery_dependencies={k: sorted(v) for k, v in subquery_deps.items()},
            )

