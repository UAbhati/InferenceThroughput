# scale-300k

Required benchmark. 300k simulated inference requests/s completed, simulated models given high limits.

Mode: bulk (in-process simulated provider, wall clock). 9 process(es) for the engine on 12 CPUs, Python 3.14.7, macOS-27.0.1-arm64-arm-64bit-Mach-O.

## Overall (measured)

- Offered: 330,000 req/s for 30.0s (warm-up 10.0s excluded from steady state)
- **Completed per second (steady state): 330,020**
- Submitted 9,898,602; completed 9,898,602 (succeeded 9,898,602, failed 0); expired 0; rejected 0; retried attempts 0
- Latency p50/p95/p99: 0.2659s / 0.5894s / 0.7944s
- Every request accounted for: **True**; all 60s windows within limits: **True**

## model-a

- Configured: 12,000,000 RPM / 16,000,000,000 TPM
- Observed (steady): 9,900,738 RPM / 9,900,022,068 TPM (82.5% / 61.9% of limit)
- Max 60s window at provider: 4,949,301 requests (worst ratio to limit in effect 0.4124), 4,949,242,062 tokens (worst ratio 0.3093)
- Latency p50/p95/p99: 0.1992s / 0.3842s / 0.5077s
- Totals: {'submitted': 4949301, 'rejected': 0, 'completed': 4949301, 'succeeded': 4949301, 'failed': 0, 'expired': 0, 'retried': 0, 'completed_tokens': 4949242062, 'waiting_at_end': 0, 'in_flight_at_end': 0, 'unaccounted': 0}

## model-b

- Configured: 12,000,000 RPM / 16,000,000,000 TPM
- Observed (steady): 9,900,459 RPM / 9,899,801,517 TPM (82.5% / 61.9% of limit)
- Max 60s window at provider: 4,949,301 requests (worst ratio to limit in effect 0.4124), 4,949,194,797 tokens (worst ratio 0.3093)
- Latency p50/p95/p99: 0.3513s / 0.6775s / 0.8863s
- Totals: {'submitted': 4949301, 'rejected': 0, 'completed': 4949301, 'succeeded': 4949301, 'failed': 0, 'expired': 0, 'retried': 0, 'completed_tokens': 4949194797, 'waiting_at_end': 0, 'in_flight_at_end': 0, 'unaccounted': 0}
