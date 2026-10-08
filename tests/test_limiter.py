import numpy as np

from gateway.limiter import SlidingWindowLimiter


def test_never_exceeds_rpm_over_any_60s():
    lim = SlidingWindowLimiter(rpm=6000, tpm=10**9)
    admitted = []  # (t, n)
    t = 0.0
    while t < 180:  # offer 2x the limit
        k = lim.admit_chunk(t, np.full(200, 1000))
        admitted.append((t, k))
        t += 0.1
    ts = np.array([a[0] for a in admitted])
    ns = np.array([a[1] for a in admitted])
    for i, t0 in enumerate(ts):
        in_window = ns[(ts > t0 - 60) & (ts <= t0)].sum()
        assert in_window <= 6000


def test_tpm_is_binding_when_tokens_are_large():
    lim = SlidingWindowLimiter(rpm=10**6, tpm=1_000_000)
    got = sum(lim.try_acquire(0.0, 10_000) for _ in range(500))
    assert got == 100  # 100 * 10k = 1M tokens


def test_capacity_returns_after_window():
    lim = SlidingWindowLimiter(rpm=100, tpm=10**9)
    assert lim.admit_chunk(0.0, np.ones(500, dtype=np.int64)) == 100
    assert lim.admit_chunk(30.0, np.ones(10, dtype=np.int64)) == 0
    assert lim.admit_chunk(60.2, np.ones(500, dtype=np.int64)) == 100


def test_reduce_limit_takes_effect_immediately_then_restore():
    lim = SlidingWindowLimiter(rpm=1000, tpm=10**9)
    lim.admit_chunk(0.0, np.ones(1000, dtype=np.int64))
    lim.set_limits(rpm=100)
    assert lim.admit_chunk(61.0, np.ones(1000, dtype=np.int64)) == 100
    lim.set_limits(rpm=1000)
    assert lim.admit_chunk(61.1, np.ones(1000, dtype=np.int64)) == 900


def test_chunk_matches_scalar():
    rng = np.random.default_rng(1)
    toks = rng.integers(100, 5000, 400)
    a, b = SlidingWindowLimiter(150, 300_000), SlidingWindowLimiter(150, 300_000)
    k = a.admit_chunk(0.0, toks)
    stopped = 0
    for t in toks:
        if not b.try_acquire(0.0, int(t)):
            break
        stopped += 1
    assert k == stopped


def test_chunk_is_fifo_prefix():
    lim = SlidingWindowLimiter(rpm=100, tpm=2500)
    # 1000 + 1000 fits, the 1000 after would exceed -> stop, even though the next 1 would fit
    assert lim.admit_chunk(0.0, np.array([1000, 1000, 1000, 1])) == 2


def test_retry_after_is_positive_when_full():
    lim = SlidingWindowLimiter(rpm=1, tpm=10**9)
    assert lim.try_acquire(5.0, 1)
    assert not lim.try_acquire(5.1, 1)
    assert 59 < lim.retry_after(5.1) <= 60.5
