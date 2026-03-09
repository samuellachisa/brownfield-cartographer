"""Vector index for semantic search over purpose statements."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from .models import ModuleNode


def _get_embedder():
    """Lazy load embedder - try OpenAI, fallback to sklearn TF-IDF."""
    try:
        import os
        from openai import OpenAI
        key = os.getenv("OPENAI_API_KEY", "")
        if key:
            client = OpenAI(api_key=key)
            def _embed(texts: List[str]) -> List[List[float]]:
                r = client.embeddings.create(
                    model=os.getenv("CARTOGRAPHER_EMBEDDING_MODEL", "text-embedding-3-small"),
                    input=texts,
                )
                return [d.embedding for d in r.data]
            return _embed
    except Exception:
        pass
    # Fallback: TF-IDF as dense vector (no external API)
    def _tfidf_embed(texts: List[str]) -> List[List[float]]:
        try:
            from sklearn.feature_extraction.text import TfidfVectorizer
            vec = TfidfVectorizer(max_features=384)
            X = vec.fit_transform(texts)
            return X.toarray().tolist()
        except Exception:
            return [[0.0] * 384 for _ in texts]
    return _tfidf_embed


class SemanticIndex:
    """In-memory vector index over module purpose statements."""

    def __init__(self) -> None:
        self._vectors: List[List[float]] = []
        self._module_paths: List[str] = []
        self._embedder = _get_embedder()

    def build(self, modules: Dict[str, ModuleNode]) -> None:
        """Index all modules with purpose statements."""
        texts = []
        paths = []
        for path, m in modules.items():
            t = m.purpose_statement or m.path
            if t:
                texts.append(t)
                paths.append(path)
        if not texts:
            self._vectors = []
            self._module_paths = []
            return
        self._vectors = self._embedder(texts)
        self._module_paths = paths

    def search(self, query: str, top_k: int = 10) -> List[Tuple[str, float]]:
        """Return (module_path, score) sorted by relevance."""
        if not self._vectors:
            return []
        qvec = self._embedder([query])[0]
        scores: List[Tuple[int, float]] = []
        for i, v in enumerate(self._vectors):
            s = _cosine_sim(qvec, v)
            scores.append((i, s))
        scores.sort(key=lambda x: x[1], reverse=True)
        return [(self._module_paths[i], s) for i, s in scores[:top_k]]

    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps({"vectors": self._vectors, "paths": self._module_paths}),
            encoding="utf-8",
        )

    def load(self, path: Path) -> bool:
        if not path.exists():
            return False
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            self._vectors = data["vectors"]
            self._module_paths = data["paths"]
            return True
        except Exception:
            return False


def _cosine_sim(a: List[float], b: List[float]) -> float:
    import math
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a)) or 1e-9
    nb = math.sqrt(sum(x * x for x in b)) or 1e-9
    return dot / (na * nb)
