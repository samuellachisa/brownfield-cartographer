from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional, Tuple, Literal

from pydantic import BaseModel, Field, field_validator


class Evidence(BaseModel):
    file: str
    line_range: Tuple[int, int]
    analysis_method: Literal["static", "llm"]
    agent: Literal["surveyor", "hydrologist", "semanticist", "archivist", "navigator"]
    confidence: float = Field(ge=0.0, le=1.0)


class ModuleNode(BaseModel):
    path: str
    language: Literal["python", "sql", "yaml", "notebook", "other"]
    purpose_statement: Optional[str] = None
    domain_cluster: Optional[str] = None
    complexity_score: float = 0.0
    change_velocity_30d: int = 0
    is_dead_code_candidate: bool = False
    last_modified: datetime
    doc_drift: Optional[Literal["aligned", "outdated", "contradictory", "missing"]] = None

    @field_validator("complexity_score")
    @classmethod
    def _non_negative_complexity(cls, v: float) -> float:
        if v < 0:
            raise ValueError("complexity_score must be non-negative")
        return v

    @field_validator("change_velocity_30d")
    @classmethod
    def _non_negative_velocity(cls, v: int) -> int:
        if v < 0:
            raise ValueError("change_velocity_30d must be non-negative")
        return v

    @field_validator("last_modified")
    @classmethod
    def _timestamp_not_far_future(cls, v: datetime) -> datetime:
        # Guard against obviously bad timestamps (e.g., year 3000) while still
        # tolerating minor clock skew.
        from datetime import datetime, timezone

        now = datetime.now(timezone.utc).astimezone(v.tzinfo) if v.tzinfo else datetime.utcnow()
        if v > now.replace(year=now.year + 1):
            raise ValueError("last_modified appears to be far in the future")
        return v


class DayOneAnswer(BaseModel):
    question: str
    answer: str
    evidence: List[Evidence] = Field(default_factory=list)


class TraceEvent(BaseModel):
    action: str
    evidence_source: Optional[str] = None
    confidence: float = 0.0
    timestamp: str = ""
    agent: str = ""


class DatasetNode(BaseModel):
    name: str
    storage_type: Literal["table", "file", "stream", "api"]
    schema_snapshot: Optional[Dict] = None
    freshness_sla: Optional[str] = None
    owner: Optional[str] = None
    is_source_of_truth: bool = False

    @field_validator("name")
    @classmethod
    def _non_empty_name(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("dataset name must be non-empty")
        return v


class FunctionNode(BaseModel):
    qualified_name: str
    parent_module: str
    signature: str
    purpose_statement: Optional[str] = None
    call_count_within_repo: int = 0
    is_public_api: bool = False

    @field_validator("call_count_within_repo")
    @classmethod
    def _non_negative_calls(cls, v: int) -> int:
        if v < 0:
            raise ValueError("call_count_within_repo must be non-negative")
        return v


class TransformationNode(BaseModel):
    source_datasets: List[str]
    target_datasets: List[str]
    transformation_type: Literal["pandas", "spark", "sql", "dbt", "airflow"]
    source_file: str
    line_range: Tuple[int, int]
    sql_query_if_applicable: Optional[str] = None


class ModuleEdgeType(str, Enum):
    IMPORTS = "IMPORTS"
    CALLS = "CALLS"
    CONFIGURES = "CONFIGURES"


class LineageEdgeType(str, Enum):
    CONSUMES = "CONSUMES"
    PRODUCES = "PRODUCES"


