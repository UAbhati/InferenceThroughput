# s3-batch-callback

Scenario 3. One asynchronous batch of 10,000 requests across two models, with simulated transient and permanent failures. The callback receiver refuses the first two delivery attempts.


Mode: http (gateway and provider simulator as separate processes, wall clock). 1 process(es) for the engine on 12 CPUs, Python 3.14.7, macOS-27.0.1-arm64-arm-64bit-Mach-O.

## Overall (measured)

- Offered: 0 req/s for 120.0s (warm-up 0.0s excluded from steady state)
- **Completed per second (steady state): 83**
- Submitted 10,000; completed 10,000 (succeeded 9,673, failed 327); expired 0; rejected 0; retried attempts 1,527
- Latency p50/p95/p99: 7.2341s / 13.5404s / 14.5171s
- Every request accounted for: **True**; all 60s windows within limits: **True**

## model-a

- Configured: 30,000 RPM / 60,000,000 TPM
- Observed (steady): 3,037 RPM / 3,037,000 TPM (10.1% / 5.1% of limit)
- Max 60s window at provider: 7,179 requests (worst ratio to limit in effect 0.2393), 7,179,000 tokens (worst ratio 0.1197)
- Latency p50/p95/p99: 7.4533s / 13.9507s / 14.6623s
- Totals: {'submitted': 6074, 'rejected': 0, 'completed': 6074, 'succeeded': 5825, 'failed': 249, 'expired': 0, 'retried': 1105, 'completed_tokens': 6074000, 'waiting_at_end': 0, 'in_flight_at_end': 0, 'unaccounted': 0}

## model-b

- Configured: 20,000 RPM / 40,000,000 TPM
- Observed (steady): 1,963 RPM / 1,963,000 TPM (9.8% / 4.9% of limit)
- Max 60s window at provider: 4,348 requests (worst ratio to limit in effect 0.2174), 4,348,000 tokens (worst ratio 0.1087)
- Latency p50/p95/p99: 6.9518s / 12.8832s / 13.5404s
- Totals: {'submitted': 3926, 'rejected': 0, 'completed': 3926, 'succeeded': 3848, 'failed': 78, 'expired': 0, 'retried': 422, 'completed_tokens': 3926000, 'waiting_at_end': 0, 'in_flight_at_end': 0, 'unaccounted': 0}

## Batch and callback

- requests: 10000
- ack_latency_s: 0.185
- time_from_ack_to_all_final_s: 18.37
- callback_attempts: [{'at_s_after_ack': 18.38, 'status_code': 503}, {'at_s_after_ack': 18.82, 'status_code': 503}, {'at_s_after_ack': 19.87, 'status_code': 200}]
- callback_final_status: delivered
- batch_status: completed_with_failures
- summary: {'batch_id': '9912cda2d21d4ce0919e49cc05315d39', 'status': 'completed_with_failures', 'total': 10000, 'succeeded': 9673, 'failed': 327, 'expired': 0}
- request_states: {'succeeded': 9673, 'failed': 327}
- progress_samples: [{'t': 0.0, 'final': 0, 'callback': 'pending'}, {'t': 1.02, 'final': 0, 'callback': 'pending'}, {'t': 2.05, 'final': 0, 'callback': 'pending'}, {'t': 3.07, 'final': 546, 'callback': 'pending'}, {'t': 4.09, 'final': 1264, 'callback': 'pending'}, {'t': 5.11, 'final': 1984, 'callback': 'pending'}, {'t': 6.14, 'final': 2739, 'callback': 'pending'}, {'t': 7.16, 'final': 3440, 'callback': 'pending'}, {'t': 8.18, 'final': 4160, 'callback': 'pending'}, {'t': 9.2, 'final': 4887, 'callback': 'pending'}, {'t': 10.22, 'final': 5602, 'callback': 'pending'}, {'t': 11.24, 'final': 6344, 'callback': 'pending'}, {'t': 12.26, 'final': 7072, 'callback': 'pending'}, {'t': 13.28, 'final': 7818, 'callback': 'pending'}, {'t': 14.3, 'final': 8524, 'callback': 'pending'}, {'t': 15.32, 'final': 9251, 'callback': 'pending'}, {'t': 16.34, 'final': 9783, 'callback': 'pending'}, {'t': 17.36, 'final': 9990, 'callback': 'pending'}, {'t': 18.38, 'final': 10000, 'callback': 'delivering'}, {'t': 19.4, 'final': 10000, 'callback': 'retrying'}]

## Pass criteria

- [x] ack_within_1s: 185 ms for 10,000 requests
- [x] callback_sent_only_after_all_final: all-final at +18.37s, first callback attempt at +18.38s
- [x] first_attempts_rejected_then_delivered: attempt status codes [503, 503, 200], gateway reports 'delivered' after 3 attempts
- [x] callback_summary_matches_batch_status: callback {'batch_id': '9912cda2d21d4ce0919e49cc05315d39', 'status': 'completed_with_failures', 'total': 10000, 'succeeded': 9673, 'failed': 327, 'expired': 0} vs API {'batch_id': '9912cda2d21d4ce0919e49cc05315d39', 'status': 'completed_with_failures', 'total': 10000, 'succeeded': 9673, 'failed': 327, 'expired': 0}
- [x] every_request_id_exactly_once: 10,000 result rows, 10,000 distinct ids, 10,000 submitted
- [x] transient_and_permanent_failures_present: states {'succeeded': 9673, 'failed': 327}; retry attempts 1,527
- [x] no_60s_window_over_limit: provider audit
