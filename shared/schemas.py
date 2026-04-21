from enum import Enum
from typing import Any, Dict, List

from pydantic import BaseModel, Field


class AgentStatus(str, Enum):
    OK = "ok"
    WARN = "warn"
    FAIL = "fail"
    HALT = "halt"


class BaseMessage(BaseModel):
    agent: str
    agent_version: str = "1.0.0"
    run_id: str
    status: AgentStatus
    reasoning: str = ""
    payload: Dict[str, Any] = Field(default_factory=dict)
    next_agents: List[str] = Field(default_factory=list)
    skip_agents: List[str] = Field(default_factory=list)
    halt: bool = False
