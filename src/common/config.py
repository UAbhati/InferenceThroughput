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


class ModelsConfig(BaseModel):
    models: dict[str, ModelSpec]

    @classmethod
    def load(cls, path: str | Path) -> "ModelsConfig":
        with open(path) as f:
            return cls.model_validate(yaml.safe_load(f))
