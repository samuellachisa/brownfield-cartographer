from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv

from ..utils.logging import get_logger

from ..graph.knowledge_graph import KnowledgeGraph
from ..models import DayOneAnswer, Evidence, ModuleNode
from ..config import CartographerConfig, load_config
from ..utils.retry import RateLimiter, retry_with_backoff

# Load .env so OPENAI_API_KEY etc. are available
load_dotenv()


@dataclass
class ContextWindowBudget:
    """Tracks token budgets for bulk vs synthesis LLM usage."""

    max_bulk: int = 500_000
    max_synthesis: int = 100_000
    used_bulk: int = field(default=0)
    used_synthesis: int = field(default=0)

    def can_spend_bulk(self, tokens: int) -> bool:
        return self.used_bulk + tokens <= self.max_bulk

    def can_spend_synthesis(self, tokens: int) -> bool:
        return self.used_synthesis + tokens <= self.max_synthesis

    def record_bulk(self, tokens: int) -> None:
        self.used_bulk += tokens

    def record_synthesis(self, tokens: int) -> None:
        self.used_synthesis += tokens


def _env(key: str, fallback_key: str | None, default: str = "") -> str:
    v = os.getenv(key, default)
    if v or not fallback_key:
        return v
    return os.getenv(fallback_key, default)


@dataclass
class LLMConfig:
    """
    Generic LLM configuration driven by environment variables.

    Supported env vars (alternatives in parentheses):
    - API key: LLM_API_KEY or OPENAI_API_KEY
    - Model: LLM_TEXT_MODEL or CARTOGRAPHER_LLM_MODEL
    - Base URL: LLM_API_BASE or CARTOGRAPHER_LLM_BASE_URL
    - RPM: CARTOGRAPHER_LLM_RPM
    """

    api_key: str = field(default_factory=lambda: _env("LLM_API_KEY", "OPENAI_API_KEY"))
    model: str = field(default_factory=lambda: _env("LLM_TEXT_MODEL", "CARTOGRAPHER_LLM_MODEL", "gpt-4o-mini"))
    base_url: str | None = field(default_factory=lambda: os.getenv("LLM_API_BASE") or os.getenv("CARTOGRAPHER_LLM_BASE_URL") or None)
    requests_per_minute: float = field(default_factory=lambda: float(os.getenv("CARTOGRAPHER_LLM_RPM", "60")))


class LLMClient:
    """
    Thin wrapper over the OpenAI Python client, but configurable via base_url
    so it can talk to OpenAI, OpenRouter, or any compatible API surface.
    """

    def __init__(self, config: LLMConfig) -> None:
        self.config = config
        if not self.config.api_key:
            raise RuntimeError(
                "LLM API key is not set. Set LLM_API_KEY or OPENAI_API_KEY in .env."
            )
        try:
            from openai import OpenAI  # type: ignore
        except ImportError as exc:  # pragma: no cover - import-time environment
            raise RuntimeError(
                "The 'openai' package is required for LLM integration. "
                "Install it with `pip install openai`."
            ) from exc

        self._client = OpenAI(
            api_key=self.config.api_key,
            base_url=self.config.base_url,
        )
        self._rate_limiter = RateLimiter(requests_per_minute=self.config.requests_per_minute)

    @retry_with_backoff(max_retries=3, base_delay=1.0, max_delay=60.0)
    def summarize_module(self, path: str, code: str) -> str:
        """
        Generate a 2–3 sentence purpose statement grounded in the provided code.
        """
        prompt = (
            "You are analyzing a data engineering codebase.\n"
            f"File path: {path}\n\n"
            "Given the following code, write a 2-3 sentence purpose statement that "
            "describes the business or data role of this module. Focus on WHAT it "
            "does, not low-level implementation details. Do not mention docstrings "
            "or speculate beyond the code shown.\n\n"
            "Code:\n"
            "```python\n"
            f"{code}\n"
            "```"
        )
        self._rate_limiter.wait()
        response = self._client.chat.completions.create(
            model=self.config.model,
            messages=[
                {"role": "system", "content": "You are a senior data platform engineer."},
                {"role": "user", "content": prompt},
            ],
            max_tokens=256,
            temperature=0.1,
        )
        content = response.choices[0].message.content or ""
        return content.strip()


@dataclass
class SemanticistConfig:
    max_modules: int = 100
    max_chars_per_module: int = 8000
    domain_clusters_k: int = 6


class SemanticistAgent:
    """
    LLM-powered purpose analyst.

    This agent reads source files for a subset of modules and uses a generic
    LLM client (configured via environment variables) to fill in
    ModuleNode.purpose_statement.
    """

    def __init__(
        self,
        config: SemanticistConfig | None = None,
        llm_config: LLMConfig | None = None,
        global_config: CartographerConfig | None = None,
    ) -> None:
        self.config = config or SemanticistConfig()
        self._llm_config = llm_config or LLMConfig()
        self._global_config = global_config
        self._budget = ContextWindowBudget()

    def _maybe_load_config(self, repo_root: Path) -> CartographerConfig:
        if self._global_config is None:
            self._global_config = load_config(repo_root)
        return self._global_config

    def _estimate_tokens(self, text: str) -> int:
        # Cheap heuristic: 1 token ~ 4 characters.
        return max(1, len(text) // 4)

    def _apply_redaction(self, text: str, cfg: CartographerConfig) -> str:
        if not cfg.privacy.redaction_patterns:
            return text
        import re

        redacted = text
        for pattern in cfg.privacy.redaction_patterns:
            try:
                redacted = re.sub(pattern, "__REDACTED__", redacted)
            except re.error:
                # Ignore invalid patterns so a bad config does not break analysis.
                continue
        return redacted

    def run(
        self,
        repo_root: Path,
        modules: Dict[str, ModuleNode],
        error_collector: Optional[List[Dict[str, Any]]] = None,
    ) -> None:
        # If no API key is set, treat as a no-op so the rest of the pipeline still works.
        if not self._llm_config.api_key:
            return

        cfg = self._maybe_load_config(repo_root)
        # Sync budget limits with global config.
        self._budget.max_bulk = cfg.budget.max_bulk_tokens
        self._budget.max_synthesis = cfg.budget.max_synthesis_tokens

        client = LLMClient(self._llm_config)

        # Prioritize modules by recent change velocity and complexity.
        sorted_modules = sorted(
            modules.values(),
            key=lambda m: (m.change_velocity_30d, m.complexity_score),
            reverse=True,
        )[: self.config.max_modules]

        for module in sorted_modules:
            if module.purpose_statement:
                continue
            file_path = repo_root / module.path
            if not file_path.is_file():
                continue
            try:
                text = file_path.read_text(encoding="utf-8", errors="ignore")
            except OSError:
                continue
            if not text.strip():
                continue

            snippet = text[: self.config.max_chars_per_module]
            snippet = self._apply_redaction(snippet, cfg)
            est_tokens = self._estimate_tokens(snippet)
            if not self._budget.can_spend_bulk(est_tokens):
                # Budget exhausted for bulk summarisation; stop early so we
                # still have headroom for synthesis tasks (Day-One answers).
                break
            try:
                purpose = client.summarize_module(module.path, snippet)
            except Exception as e:
                get_logger("semanticist").warning(
                    "Skipping module (LLM failed): %s - %s",
                    module.path,
                    e,
                    extra={"agent": "semanticist", "file": module.path, "error": str(e)},
                )
                if error_collector is not None:
                    error_collector.append({
                        "agent": "semanticist",
                        "file": module.path,
                        "error": str(e),
                    })
                continue
            self._budget.record_bulk(est_tokens)
            if purpose:
                module.purpose_statement = purpose

        self._detect_doc_drift(repo_root, modules)
        self._cluster_into_domains(modules)

    def _extract_docstring(self, code: str) -> Optional[str]:
        m = re.search(r'"""([^"]*)"""|\'\'\'([^\']*)\'\'\'', code, re.DOTALL)
        if m:
            return (m.group(1) or m.group(2) or "").strip() or None
        return None

    def _detect_doc_drift(self, repo_root: Path, modules: Dict[str, ModuleNode]) -> None:
        """Flag docstrings that contradict implementation. Requires LLM."""
        if not self._llm_config.api_key:
            return
        try:
            client = LLMClient(self._llm_config)
        except Exception:
            return
        for module in list(modules.values())[:20]:
            if module.language != "python":
                continue
            path = repo_root / module.path
            if not path.is_file():
                continue
            try:
                code = path.read_text(encoding="utf-8", errors="ignore")[:4000]
            except OSError:
                continue
            docstring = self._extract_docstring(code)
            if not docstring:
                module.doc_drift = "missing"
                continue
            prompt = (
                f"File: {module.path}\n\n"
                f"Docstring: {docstring}\n\n"
                f"Code (excerpt):\n```\n{code[:2000]}\n```\n\n"
                "Does the docstring accurately describe what the code does? "
                "Reply with one word: aligned, outdated, or contradictory."
            )
            try:
                client._rate_limiter.wait()
                r = client._client.chat.completions.create(
                    model=client.config.model,
                    messages=[{"role": "user", "content": prompt}],
                    max_tokens=20,
                    temperature=0,
                )
                label = (r.choices[0].message.content or "aligned").strip().lower()
                if "contradictory" in label:
                    module.doc_drift = "contradictory"
                elif "outdated" in label:
                    module.doc_drift = "outdated"
                else:
                    module.doc_drift = "aligned"
            except Exception:
                pass

    def _cluster_into_domains(self, modules: Dict[str, ModuleNode]) -> None:
        """Cluster modules by purpose statement similarity (TF-IDF + k-means)."""
        try:
            from sklearn.cluster import KMeans
            from sklearn.feature_extraction.text import TfidfVectorizer
        except ImportError:
            return
        texts = []
        paths = []
        for p, m in modules.items():
            t = m.purpose_statement or m.path
            if t and t.strip():
                texts.append(t)
                paths.append(p)
        if len(texts) < 2:
            return
        k = min(self.config.domain_clusters_k, len(texts) - 1, 8)
        if k < 2:
            return
        try:
            vec = TfidfVectorizer(max_features=200, stop_words="english")
            X = vec.fit_transform(texts)
            km = KMeans(n_clusters=k, random_state=42, n_init=10)
            labels = km.fit_predict(X)
            domain_names = [
                "ingestion",
                "transformation",
                "serving",
                "monitoring",
                "orchestration",
                "utilities",
                "config",
                "testing",
            ]
            for i, path in enumerate(paths):
                if path in modules:
                    lid = labels[i] if i < len(labels) else 0
                    modules[path].domain_cluster = domain_names[lid % len(domain_names)]
        except Exception:
            pass

