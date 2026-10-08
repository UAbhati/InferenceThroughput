"""The demo console, end to end: real provider and gateway processes behind it, real HTTP."""
import asyncio
import time

import httpx
import uvicorn

from loadgen.console import create_console
from loadgen.http_run import free_port

MODELS = """
models:
  model-a: {rpm: 30000, tpm: 60000000, latency_ms_median: 20, latency_sigma: 0}
  model-b: {rpm: 20000, tpm: 40000000, latency_ms_median: 20, latency_sigma: 0}
engine: {queue_max: 20000, queue_ttl_s: 30, headroom: 0.98}
gateway: {callback_backoff_base_s: 0.1, callback_backoff_cap_s: 0.3}
"""


async def test_console_drives_load_limits_batches_and_callbacks(tmp_path):
    models = tmp_path / "models.yaml"
    models.write_text(MODELS)
    port = free_port()
    app = create_console(str(models), port, tmp_path)
    server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning"))
    task = asyncio.get_running_loop().create_task(server.serve())
    try:
        async with httpx.AsyncClient(base_url=f"http://127.0.0.1:{port}", timeout=30) as c:
            for _ in range(200):
                try:
                    if (await c.get("/api/state")).json()["gateway_up"]:
                        break
                except (httpx.HTTPError, KeyError):
                    pass
                await asyncio.sleep(0.2)
            else:
                raise AssertionError("console did not come up")
            st = (await c.get("/api/state")).json()
            assert set(st["models"]) == {"model-a", "model-b"} and "/docs" in st["links"]["gateway_docs"]
            assert "Inference gateway console" in (await c.get("/")).text

            # load, with a limit change in the middle
            await c.post("/api/load/start", json={"rate_per_s": 300, "token_size": 1000, "mix": {"model-a": 1, "model-b": 1}})
            await asyncio.sleep(2)
            r = await c.put("/api/limits", json={"model": "model-a", "rpm": 6000})
            assert r.json()["rpm"] == 6000
            await asyncio.sleep(1)
            await c.post("/api/load/stop")
            st = (await c.get("/api/state")).json()
            assert st["models"]["model-a"]["limits"]["rpm"] == 6000 and st["models"]["model-b"]["limits"]["rpm"] == 20000
            assert st["models"]["model-a"]["provider"]["limit_rpm"] == 6000  # the provider was changed too
            assert st["load"]["sent"] > 300 and st["load"]["errors"] == 0

            # failure injection reaches the provider
            assert (await c.put("/api/provider/behavior", json={"model": "model-b", "transient_failure_rate": 0.2})).json()["transient_failure_rate"] == 0.2

            # let it go idle, then batch + callback that is refused once
            for _ in range(100):
                s = (await c.get("/api/state")).json()
                if sum(m["queued"] + m["in_flight"] for m in s["models"].values()) == 0:
                    break
                await asyncio.sleep(0.2)
            b = (await c.post("/api/batch", json={"total": 300, "mix": {"model-a": 1, "model-b": 1}, "reject_first_callbacks": 1})).json()
            end = time.time() + 60
            while time.time() < end:
                s = (await c.get("/api/state")).json()
                if s["batches"] and s["batches"][-1]["callback"]["status"] == "delivered":
                    break
                await asyncio.sleep(0.3)
            batch = s["batches"][-1]
            assert batch["id"] == b["id"] and batch["succeeded"] + batch["failed"] == 300 and batch["callback"]["attempts"] == 2
            assert [x["status_code"] for x in s["callbacks"]] == [503, 200]
    finally:
        server.should_exit = True
        await task
