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
    burst_s: float = Field(default=0.25, gt=0, description="smoothing: never send more than this many seconds of "
                                                         "capacity at once (the sliding window alone would allow a full minute)")


class GatewayConfig(BaseModel):
    """Service-level settings (batches, callbacks)."""
    batch_max_requests: int = 100_000
    batch_queue_fraction: float = Field(default=0.5, gt=0, le=1, description="share of a model queue batches may fill")
    batch_target_ttl_fraction: float = Field(default=0.5, gt=0, le=1,
                                             description="feed batches no deeper than this fraction of ttl worth of capacity")
    callback_max_attempts: int = 8
    callback_backoff_base_s: float = 0.5
    callback_backoff_cap_s: float = 15.0
    callback_timeout_s: float = 5.0
    callback_inline_results_max: int = 100
    provider_timeout_s: float = 60.0


class ModelsConfig(BaseModel):
    models: dict[str, ModelSpec]
    engine: EngineConfig = EngineConfig()
    gateway: GatewayConfig = GatewayConfig()

    @classmethod
    def load(cls, path: str | Path) -> "ModelsConfig":
        with open(path) as f:
            return cls.model_validate(yaml.safe_load(f))
