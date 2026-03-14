"""Structured logging for cartographer runs."""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime
from typing import Any, Dict, Optional


def _serialize(obj: Any) -> Any:
    if hasattr(obj, "isoformat"):
        return obj.isoformat()
    if isinstance(obj, (str, int, float, bool, type(None))):
        return obj
    if isinstance(obj, (list, tuple)):
        return [_serialize(x) for x in obj]
    if isinstance(obj, dict):
        return {k: _serialize(v) for k, v in obj.items()}
    return str(obj)


class StructuredFormatter(logging.Formatter):
    """JSON-structured log formatter for SIEM/observability."""

    def format(self, record: logging.LogRecord) -> str:
        payload: Dict[str, Any] = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": record.levelname,
            "message": record.getMessage(),
            "logger": record.name,
        }
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        if hasattr(record, "run_id"):
            payload["run_id"] = record.run_id
        if hasattr(record, "agent"):
            payload["agent"] = record.agent
        if hasattr(record, "duration"):
            payload["duration_ms"] = record.duration
        if hasattr(record, "extra"):
            payload["extra"] = _serialize(record.extra)
        return json.dumps(payload)


def setup_logging(verbose: bool = False, json_logs: bool = False) -> None:
    """Configure logging. If json_logs, use structured JSON to stdout."""
    root = logging.getLogger("cartographer")
    root.setLevel(logging.DEBUG if verbose else logging.INFO)
    if not root.handlers:
        h = logging.StreamHandler(sys.stdout)
        h.setLevel(logging.DEBUG if verbose else logging.INFO)
        if json_logs:
            h.setFormatter(StructuredFormatter())
        else:
            h.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
        root.addHandler(h)


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(f"cartographer.{name}")


def log_file_skip(
    logger: logging.Logger,
    agent: str,
    file_path: str,
    error: Exception,
    run_id: Optional[str] = None,
    error_collector: Optional[list] = None,
) -> None:
    """Log a skipped file and optionally collect for traces."""
    msg = str(error)
    extra: Dict[str, Any] = {"agent": agent, "file": file_path, "error": msg}
    if run_id:
        extra["run_id"] = run_id
    logger.warning("Skipping file due to error: %s", msg, extra=extra)
    if error_collector is not None:
        error_collector.append({
            "agent": agent,
            "file": file_path,
            "error": msg,
        })
