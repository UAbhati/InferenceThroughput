# s3-batch-callback

Scenario 3. One asynchronous batch of 10,000 requests across two models, with simulated transient and permanent failures. The callback receiver refuses the first two delivery attempts.


Mode: http (gateway and provider simulator as separate processes, wall clock). 1 process(es) for the engine on 12 CPUs, Python 3.14.7, macOS-27.0.1-arm64-arm-64bit-Mach-O.

## Overall (measured)

- Offered: 0 req/s for 120.0s (warm-up 0.0s excluded from steady state)
- **Completed per second (steady state): 83**
- Submitted 10,000; completed 10,000 (succeeded 9,669, failed 331); expired 0; rejected 0; retried attempts 1,507
- Latency p50/p95/p99: 7.2341s / 13.5404s / 14.3734s
- Every request accounted for: **True**; all 60s windows within limits: **True**

## model-a

- Configured: 30,000 RPM / 60,000,000 TPM
- Observed (steady): 3,037 RPM / 3,037,000 TPM (10.1% / 5.1% of limit)
- Max 60s window at provider: 7,121 requests (worst ratio to limit in effect 0.2374), 7,121,000 tokens (worst ratio 0.1187)
- Latency p50/p95/p99: 7.3795s / 13.8126s / 14.5171s
- Totals: {'submitted': 6074, 'rejected': 0, 'completed': 6074, 'succeeded': 5842, 'failed': 232, 'expired': 0, 'retried': 1047, 'completed_tokens': 6074000, 'waiting_at_end': 0, 'in_flight_at_end': 0, 'unaccounted': 0}

## model-b

- Configured: 20,000 RPM / 40,000,000 TPM
- Observed (steady): 1,963 RPM / 1,963,000 TPM (9.8% / 4.9% of limit)
- Max 60s window at provider: 4,386 requests (worst ratio to limit in effect 0.2193), 4,386,000 tokens (worst ratio 0.1096)
- Latency p50/p95/p99: 7.0214s / 13.0121s / 13.6758s
- Totals: {'submitted': 3926, 'rejected': 0, 'completed': 3926, 'succeeded': 3827, 'failed': 99, 'expired': 0, 'retried': 460, 'completed_tokens': 3926000, 'waiting_at_end': 0, 'in_flight_at_end': 0, 'unaccounted': 0}

## Batch and callback

- requests: 10000
- ack_latency_s: 0.188
- time_from_ack_to_all_final_s: 17.65
- callback_attempts: [{'at_s_after_ack': 17.67, 'status_code': 503}, {'at_s_after_ack': 18.14, 'status_code': 503}, {'at_s_after_ack': 19.3, 'status_code': 200}]
- callback_final_status: delivered
- batch_status: completed_with_failures
- summary: {'batch_id': 'dab7744277d1450f9a98ce0ba015a2a9', 'status': 'completed_with_failures', 'total': 10000, 'succeeded': 9669, 'failed': 331, 'expired': 0}
- request_states: {'succeeded': 9669, 'failed': 331}
- progress_samples: [{'t': 0.0, 'final': 0, 'callback': 'pending'}, {'t': 1.03, 'final': 0, 'callback': 'pending'}, {'t': 2.05, 'final': 0, 'callback': 'pending'}, {'t': 3.07, 'final': 528, 'callback': 'pending'}, {'t': 4.09, 'final': 1231, 'callback': 'pending'}, {'t': 5.12, 'final': 1981, 'callback': 'pending'}, {'t': 6.14, 'final': 2724, 'callback': 'pending'}, {'t': 7.16, 'final': 3443, 'callback': 'pending'}, {'t': 8.18, 'final': 4160, 'callback': 'pending'}, {'t': 9.2, 'final': 4896, 'callback': 'pending'}, {'t': 10.22, 'final': 5597, 'callback': 'pending'}, {'t': 11.24, 'final': 6335, 'callback': 'pending'}, {'t': 12.27, 'final': 7066, 'callback': 'pending'}, {'t': 13.29, 'final': 7814, 'callback': 'pending'}, {'t': 14.31, 'final': 8522, 'callback': 'pending'}, {'t': 15.32, 'final': 9256, 'callback': 'pending'}, {'t': 16.35, 'final': 9839, 'callback': 'pending'}, {'t': 17.37, 'final': 9996, 'callback': 'pending'}, {'t': 18.39, 'final': 10000, 'callback': 'retrying'}, {'t': 19.41, 'final': 10000, 'callback': 'delivered'}]

## Pass criteria

- [x] ack_within_1s: 188 ms for 10,000 requests
- [x] callback_sent_only_after_all_final: all-final at +17.65s, first callback attempt at +17.67s
- [x] first_attempts_rejected_then_delivered: attempt status codes [503, 503, 200], gateway reports 'delivered' after 3 attempts
- [x] callback_summary_matches_batch_status: callback {'batch_id': 'dab7744277d1450f9a98ce0ba015a2a9', 'status': 'completed_with_failures', 'total': 10000, 'succeeded': 9669, 'failed': 331, 'expired': 0} vs API {'batch_id': 'dab7744277d1450f9a98ce0ba015a2a9', 'status': 'completed_with_failures', 'total': 10000, 'succeeded': 9669, 'failed': 331, 'expired': 0}
- [x] every_request_id_exactly_once: 10,000 result rows, 10,000 distinct ids, 10,000 submitted
- [x] transient_and_permanent_failures_present: states {'succeeded': 9669, 'failed': 331}; retry attempts 1,507
- [x] no_60s_window_over_limit: provider audit
