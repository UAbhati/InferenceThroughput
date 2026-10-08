"""Scenario files describe one repeatable run: models, offered load, limit changes."""
from __future__ import annotations

from pathlib import Path

import yaml
from pydantic import BaseModel, Field

from common.config import EngineConfig, ModelSpec


class LimitChange(BaseModel):
    at_s: float
    model: str
    rpm: int | None = None
    tpm: int | None = None


class Load(BaseModel):
    rate_per_s: float = Field(gt=0, description="offered requests per second, all models together")
    mix: dict[str, float] = Field(description="model -> share of traffic")
    token_size: int = 1000
    token_jitter: float = Field(default=0.0, ge=0, lt=1, description="+/- fraction around token_size")
    batch_size: int = 1


class Scenario(BaseModel):
    name: str
    description: str = ""
    duration_s: float
    warmup_s: float = 0.0
    drain_s: float = 15.0
    models: dict[str, ModelSpec]
    load: Load
    changes: list[LimitChange] = []
    engine: EngineConfig = EngineConfig()
    procs: int | None = None

    @classmethod
    def load_file(cls, path: str | Path) -> "Scenario":
        with open(path) as f:
            return cls.model_validate(yaml.safe_load(f))
