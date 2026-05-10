"""Backward-compatibility test for ghg-mapper-enhancements.

Feature: ghg-mapper-enhancements
Property 8: Backward compatibility — all-flags-off equals legacy
Validates: Requirement 19.1
"""

import math
import numpy as np
import pytest
from hypothesis import given, settings, strategies as st

from ghg_mapper.pipeline.methodology import grid_inverse_variance


def _legacy_arithmetic_mean_grid(lats, lons, values, bbox, grid_res):
    """Reference implementation of the legacy _grid_oco_to_tif gridding."""
    west, south, east, north = bbox
    nx = max(1, int(round((east - west) / grid_res)))
    ny = max(1, int(round((north - south) / grid_res)))
    grid_sum = np.zeros((ny, nx), dtype=np.float64)
    grid_cnt = np.zeros((ny, nx), dtype=np.int64)
    for lat, lon, val in zip(lats, lons, values):
        xi = int((lon - west) / grid_res)
        yi = int((north - lat) / grid_res)
        if 0 <= xi < nx and 0 <= yi < ny:
            grid_sum[yi, xi] += val
            grid_cnt[yi, xi] += 1
    with np.errstate(invalid="ignore", divide="ignore"):
        grid_mean = np.where(grid_cnt > 0, grid_sum / grid_cnt, np.nan)
    return grid_mean, grid_cnt


def test_iv_with_constant_sigma_equals_legacy_arithmetic_mean_fixed():
    # Fixed small example with a bbox whose span is an exact multiple of
    # grid_res in floating-point, so legacy (round) and IV (ceil) agree on
    # the output shape — no alignment slack needed.
    np.random.seed(42)
    bbox = [73.0, 24.0, 74.0, 25.0]   # 1.0° span / 0.1° = 10 exactly
    grid_res = 0.1
    n = 50
    # Keep points strictly inside the bbox so both kernels place them in
    # the same row/column for this fixed test.
    lats = np.random.uniform(24.01, 24.99, n)
    lons = np.random.uniform(73.01, 73.99, n)
    values = np.random.uniform(400.0, 420.0, n)
    sigmas = np.ones(n)
    legacy_mean, legacy_cnt = _legacy_arithmetic_mean_grid(lats, lons, values, bbox, grid_res)
    iv_mean, iv_stderr, iv_cnt = grid_inverse_variance(
        lats, lons, values, sigmas, bbox, grid_res, min_retrievals=1,
    )
    assert iv_cnt.shape == legacy_cnt.shape, (
        "ceil vs round dimension mismatch — widen bbox or pick a round grid_res"
    )
    np.testing.assert_array_equal(iv_cnt, legacy_cnt)
    # NaN masks must align
    assert np.array_equal(np.isnan(iv_mean), np.isnan(legacy_mean))
    # Finite cells must match within FP tolerance
    finite = ~np.isnan(legacy_mean)
    np.testing.assert_allclose(iv_mean[finite], legacy_mean[finite], rtol=1e-10, atol=1e-10)


@settings(max_examples=50, deadline=None)
@given(
    seed=st.integers(min_value=0, max_value=10_000),
    n=st.integers(min_value=1, max_value=200),
    west=st.floats(min_value=-170.0, max_value=150.0, allow_nan=False, allow_infinity=False),
    south=st.floats(min_value=-80.0, max_value=60.0, allow_nan=False, allow_infinity=False),
    span=st.floats(min_value=0.5, max_value=20.0, allow_nan=False, allow_infinity=False),
    grid_res=st.sampled_from([0.1, 0.25, 0.5, 1.0]),
)
def test_iv_with_constant_sigma_equals_legacy_fuzz(seed, n, west, south, span, grid_res):
    east = west + span
    north = south + span
    bbox = [west, south, east, north]
    rng = np.random.default_rng(seed)
    lats = rng.uniform(south, north, n)
    lons = rng.uniform(west, east, n)
    values = rng.uniform(100.0, 500.0, n)
    sigmas = np.ones(n)
    legacy_mean, legacy_cnt = _legacy_arithmetic_mean_grid(lats, lons, values, bbox, grid_res)
    iv_mean, _, iv_cnt = grid_inverse_variance(
        lats, lons, values, sigmas, bbox, grid_res, min_retrievals=1,
    )
    # Shape may differ by 1 at the boundary (legacy uses round, grid_inverse_variance uses ceil).
    # Align on the smaller shape.
    ry = min(legacy_mean.shape[0], iv_mean.shape[0])
    rx = min(legacy_mean.shape[1], iv_mean.shape[1])
    lm = legacy_mean[:ry, :rx]
    im = iv_mean[:ry, :rx]
    lc = legacy_cnt[:ry, :rx]
    ic = iv_cnt[:ry, :rx]
    # Count grid: should match within the overlap region.
    np.testing.assert_array_equal(ic, lc)
    # Mean: compare only where BOTH sides have count > 0.
    both_finite = (~np.isnan(lm)) & (~np.isnan(im))
    if both_finite.any():
        np.testing.assert_allclose(im[both_finite], lm[both_finite], rtol=1e-10, atol=1e-10)


def test_iv_count_grid_matches_legacy_count_grid():
    """Explicit verification that grid_inverse_variance count_grid matches
    the legacy grid_cnt exactly for a non-trivial distribution of points."""
    np.random.seed(2024)
    bbox = [72.0, 22.0, 78.0, 28.0]
    grid_res = 0.25
    n = 500
    lats = np.random.uniform(22.0, 28.0, n)
    lons = np.random.uniform(72.0, 78.0, n)
    values = np.random.uniform(390.0, 430.0, n)
    sigmas = np.ones(n)
    _, legacy_cnt = _legacy_arithmetic_mean_grid(lats, lons, values, bbox, grid_res)
    _, _, iv_cnt = grid_inverse_variance(
        lats, lons, values, sigmas, bbox, grid_res, min_retrievals=1,
    )
    # Total retrievals preserved across both implementations.
    assert int(iv_cnt.sum()) == int(legacy_cnt.sum()) == n
    # Per-cell counts match exactly.
    np.testing.assert_array_equal(iv_cnt, legacy_cnt)
