from __future__ import annotations

from datetime import datetime
from typing import Dict, List, Optional, Tuple, Literal

from pydantic import BaseModel, Field


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


class FunctionNode(BaseModel):
    qualified_name: str
    parent_module: str
    signature: str
    purpose_statement: Optional[str] = None
    call_count_within_repo: int = 0
    is_public_api: bool = False


class TransformationNode(BaseModel):
    source_datasets: List[str]
    target_datasets: List[str]
    transformation_type: Literal["pandas", "spark", "sql", "dbt", "airflow"]
    source_file: str
    line_range: Tuple[int, int]
    sql_query_if_applicable: Optional[str] = None

