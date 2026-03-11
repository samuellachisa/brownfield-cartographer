from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import List


@dataclass
class PrivacyConfig:
    """Controls basic privacy/safety behaviour for analysis."""

    redaction_patterns: List[str] = field(default_factory=list)


@dataclass
class BudgetConfig:
    """Global token / work-budget configuration for LLM usage."""

    max_bulk_tokens: int = 500_000
    max_synthesis_tokens: int = 100_000


@dataclass
class SQLConfig:
    """SQL dialect hints, per-path overrides, etc."""

    default_dialect: str | None = None

    def for_file(self, path: Path) -> str | None:
        # For now just return the default; can be extended to inspect path.
        return self.default_dialect


@dataclass
class CartographerConfig:
    """
    Central configuration for Brownfield Cartographer.

    This intentionally stays minimal; agents only rely on a few fields
    (ignore_dirs, budget, privacy, sql dialect hints). The defaults are
    chosen to be safe and work out-of-the-box.
    """

    ignore_dirs: List[str] = field(
        default_factory=lambda: [
            ".git",
            ".venv",
            "venv",
            ".mypy_cache",
            ".pytest_cache",
            "__pycache__",
            ".cartography",
            "node_modules",
        ]
    )
    budget: BudgetConfig = field(default_factory=BudgetConfig)
    privacy: PrivacyConfig = field(default_factory=PrivacyConfig)
    sql: SQLConfig = field(default_factory=SQLConfig)


def load_config(repo_root: Path | None = None) -> CartographerConfig:
    """
    Load Cartographer configuration for a given repository root.

    For now this simply returns the default configuration; in the future it
    could read a `cartographer.yaml` or similar file from repo_root.
    """

    # `repo_root` is accepted for future compatibility but currently unused.
    _ = repo_root
    return CartographerConfig()

