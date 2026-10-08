# scale-1m

Stretch benchmark. 1M simulated inference requests/s completed, simulated models given high limits.

Mode: bulk (in-process simulated provider, wall clock). 12 process(es) for the engine on 12 CPUs, Python 3.14.7, macOS-27.0.1-arm64-arm-64bit-Mach-O.

## Overall (measured)

- Offered: 1,100,000 req/s for 30.0s (warm-up 10.0s excluded from steady state)
- **Completed per second (steady state): 1,099,978**
- Submitted 32,997,996; completed 32,997,996 (succeeded 32,997,996, failed 0); expired 0; rejected 0; retried attempts 0
- Latency p50/p95/p99: 0.2659s / 0.5894s / 0.7944s
- Every request accounted for: **True**; all 60s windows within limits: **True**

## model-a

- Configured: 40,000,000 RPM / 50,000,000,000 TPM
- Observed (steady): 33,000,021 RPM / 33,000,637,203 TPM (82.5% / 66.0% of limit)
- Max 60s window at provider: 16,498,998 requests (worst ratio to limit in effect 0.4125), 16,498,677,065 tokens (worst ratio 0.3300)
- Latency p50/p95/p99: 0.1992s / 0.3881s / 0.5077s
- Totals: {'submitted': 16498998, 'rejected': 0, 'completed': 16498998, 'succeeded': 16498998, 'failed': 0, 'expired': 0, 'retried': 0, 'completed_tokens': 16498677065, 'waiting_at_end': 0, 'in_flight_at_end': 0, 'unaccounted': 0}

## model-b

- Configured: 40,000,000 RPM / 50,000,000,000 TPM
- Observed (steady): 32,998,650 RPM / 32,997,632,325 TPM (82.5% / 66.0% of limit)
- Max 60s window at provider: 16,498,998 requests (worst ratio to limit in effect 0.4125), 16,498,836,704 tokens (worst ratio 0.3300)
- Latency p50/p95/p99: 0.3513s / 0.6775s / 0.8863s
- Totals: {'submitted': 16498998, 'rejected': 0, 'completed': 16498998, 'succeeded': 16498998, 'failed': 0, 'expired': 0, 'retried': 0, 'completed_tokens': 16498836704, 'waiting_at_end': 0, 'in_flight_at_end': 0, 'unaccounted': 0}
