"""Model configuration: limits plus the simulated provider behaviour for each model."""
from __future__ import annotations

from pathlib import Path

import yaml
from pydantic import BaseModel, Field


class ModelSpec(BaseModel):
    rpm: int = Field(gt=0, description="requests per minute")
    tpm: int = Field(gt=0, description="tokens per minute")
    # Simulated provider behaviour (ignored by the gateway itself)
    latency_ms_median: float = 300.0
    latency_sigma: float = 0.4  # lognormal shape; 0 = constant latency
    transient_failure_rate: float = Field(default=0.0, ge=0, le=1)
    permanent_failure_rate: float = Field(default=0.0, ge=0, le=1)


class EngineConfig(BaseModel):
    """Gateway behaviour when demand meets (or exceeds) capacity."""
    queue_max: int = Field(default=100_000, gt=0, description="bounded queue per model; beyond it submits are rejected")
    queue_ttl_s: float = Field(default=30.0, gt=0, description="a request still queued after this long expires")
    max_attempts: int = Field(default=3, ge=1, description="attempts per request incl. retries of transient failures")
    headroom: float = Field(default=1.0, gt=0, le=1, description="fraction of the provider limit the gateway uses")
    tick_s: float = Field(default=0.005, gt=0, description="dispatch loop period")


class ModelsConfig(BaseModel):
    models: dict[str, ModelSpec]

    @classmethod
    def load(cls, path: str | Path) -> "ModelsConfig":
        with open(path) as f:
            return cls.model_validate(yaml.safe_load(f))
