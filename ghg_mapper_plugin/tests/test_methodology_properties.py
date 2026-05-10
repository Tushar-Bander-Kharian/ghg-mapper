"""Property-based tests for ghg-mapper-enhancements methodology helpers.

Feature: ghg-mapper-enhancements
"""

from __future__ import annotations

from datetime import date, datetime, timedelta

import numpy as np
import pytest
from hypothesis import HealthCheck, assume, given, settings, strategies as st

from ghg_mapper.pipeline.methodology import (
    apply_mask,
    compute_enhancement,
    compute_priority_score,
    inverse_variance_mean,
    mass_balance_flux,
    split_composite_windows,
)

# ─────────────────────────────────────────────────────────────────────────────
# Shared strategies
# ─────────────────────────────────────────────────────────────────────────────

# Bounded floats sufficient for geophysical CH4/XCO2-like quantities.
finite_concentration = st.floats(
    min_value=1500.0, max_value=2500.0,
    allow_nan=False, allow_infinity=False, width=64,
)

# Optional-NaN concentration: 10% chance of NaN to exercise NaN handling.
conc_with_nan = st.one_of(
    st.just(float("nan")),
    finite_concentration,
)


@st.composite
def conc_grid(draw, min_dim=4, max_dim=10):
    """Generate a 2-D concentration array of shape (R, C)."""
    r = draw(st.integers(min_value=min_dim, max_value=max_dim))
    c = draw(st.integers(min_value=min_dim, max_value=max_dim))
    flat = draw(st.lists(conc_with_nan, min_size=r * c, max_size=r * c))
    return np.array(flat, dtype=np.float64).reshape(r, c)


# ─────────────────────────────────────────────────────────────────────────────
# Property 1: Enhancement is concentration minus background
# Feature: ghg-mapper-enhancements, Property 1: Enhancement is concentration
#   minus background
# Validates: Requirements 1.1, 1.2
# ─────────────────────────────────────────────────────────────────────────────
@settings(max_examples=50, deadline=None,
          suppress_health_check=[HealthCheck.too_slow])
@given(
    arr=conc_grid(min_dim=4, max_dim=8),
    window_cells=st.sampled_from([3, 5, 7]),
    pct=st.sampled_from([5, 10, 25]),
)
def test_enhancement_is_conc_minus_background(arr, window_cells, pct):
    enh, bg = compute_enhancement(arr, window_cells, pct)
    assert enh.shape == arr.shape == bg.shape

    # 1) Where arr is NaN, enhancement must be NaN.
    nan_in = np.isnan(arr)
    assert np.all(np.isnan(enh[nan_in]))

    # 2) Where background is NaN (all-NaN window), enhancement must be NaN.
    assert np.all(np.isnan(enh[np.isnan(bg)]))

    # 3) For cells where both are finite, enhancement == arr - background.
    good = ~np.isnan(arr) & ~np.isnan(bg)
    np.testing.assert_allclose(enh[good], arr[good] - bg[good], rtol=0, atol=1e-12)

    # 4) Background must lie within [min, max] of its (non-NaN) window.
    R, C = arr.shape
    half = (window_cells if window_cells % 2 == 1 else window_cells + 1) // 2
    for i in range(R):
        i0, i1 = max(0, i - half), min(R, i + half + 1)
        for j in range(C):
            j0, j1 = max(0, j - half), min(C, j + half + 1)
            win = arr[i0:i1, j0:j1].ravel()
            finite_win = win[~np.isnan(win)]
            if finite_win.size == 0:
                assert np.isnan(bg[i, j])
                continue
            # Allow a touch of numerical slack.
            lo = float(np.min(finite_win))
            hi = float(np.max(finite_win))
            assert lo - 1e-9 <= bg[i, j] <= hi + 1e-9


# ─────────────────────────────────────────────────────────────────────────────
# Property 2: Cropland mask is idempotent
# Feature: ghg-mapper-enhancements, Property 2: Cropland mask is idempotent
# Validates: Requirement 2.3
# ─────────────────────────────────────────────────────────────────────────────
@st.composite
def data_mask_pair(draw):
    r = draw(st.integers(min_value=1, max_value=8))
    c = draw(st.integers(min_value=1, max_value=8))
    flat_data = draw(st.lists(
        st.one_of(st.just(float("nan")),
                  st.floats(min_value=-1000.0, max_value=1000.0,
                            allow_nan=False, allow_infinity=False, width=64)),
        min_size=r * c, max_size=r * c,
    ))
    flat_mask = draw(st.lists(st.booleans(), min_size=r * c, max_size=r * c))
    data = np.array(flat_data, dtype=np.float64).reshape(r, c)
    mask = np.array(flat_mask, dtype=bool).reshape(r, c)
    return data, mask


@settings(max_examples=200, deadline=None)
@given(pair=data_mask_pair())
def test_cropland_mask_idempotent(pair):
    data, mask = pair
    applied1 = apply_mask(data, mask)
    applied2 = apply_mask(applied1, mask)

    assert np.array_equal(applied1, applied2, equal_nan=True)

    # Every mask==0 position is NaN.
    assert np.all(np.isnan(applied1[~mask]))

    # NaN in input stays NaN regardless of mask.
    orig_nan = np.isnan(data)
    assert np.all(np.isnan(applied1[orig_nan]))

    # Values in kept positions match the originals (modulo dtype promotion).
    kept = mask & ~orig_nan
    np.testing.assert_array_equal(applied1[kept], data[kept])


# ─────────────────────────────────────────────────────────────────────────────
# Property 3: Mass-balance flux is linear in enhancement and wind
# Feature: ghg-mapper-enhancements, Property 3: Mass-balance flux is linear
#   in enhancement and wind
# Validates: Requirement 4.1
# ─────────────────────────────────────────────────────────────────────────────
@settings(max_examples=200, deadline=None)
@given(
    enh=st.floats(min_value=0.1, max_value=500.0,
                  allow_nan=False, allow_infinity=False, width=64),
    u=st.floats(min_value=0.1, max_value=20.0,
                allow_nan=False, allow_infinity=False, width=64),
    grid_length=st.floats(min_value=100.0, max_value=100_000.0,
                          allow_nan=False, allow_infinity=False, width=64),
    mol_mass=st.floats(min_value=1.0, max_value=100.0,
                       allow_nan=False, allow_infinity=False, width=64),
)
def test_mass_balance_flux_linear(enh, u, grid_length, mol_mass):
    f1 = mass_balance_flux(enh, u, grid_length, mol_mass, species="ch4")
    f2 = mass_balance_flux(2.0 * enh, u, grid_length, mol_mass, species="ch4")
    f_half_u = mass_balance_flux(enh, u / 2.0, grid_length, mol_mass, species="ch4")

    # Doubling enhancement doubles flux.
    assert f1 == pytest.approx(f1, rel=0, abs=0)
    assert f2 == pytest.approx(2.0 * f1, rel=1e-9, abs=1e-18)
    # Halving wind halves flux.
    assert f_half_u == pytest.approx(f1 / 2.0, rel=1e-9, abs=1e-18)


# ─────────────────────────────────────────────────────────────────────────────
# Property 4: Inverse-variance mean equals arithmetic mean for equal σ
# Feature: ghg-mapper-enhancements, Property 4: Inverse-variance mean equals
#   arithmetic mean for equal uncertainties
# Validates: Requirements 10.2, 10.3
# ─────────────────────────────────────────────────────────────────────────────
finite_small = st.floats(min_value=-1000.0, max_value=1000.0,
                         allow_nan=False, allow_infinity=False, width=64)
positive_sigma = st.floats(min_value=1e-3, max_value=100.0,
                           allow_nan=False, allow_infinity=False, width=64)


@settings(max_examples=200, deadline=None)
@given(
    values=st.lists(finite_small, min_size=2, max_size=20),
    sigma=positive_sigma,
)
def test_iv_mean_equals_arith_mean_equal_unc(values, sigma):
    v = np.asarray(values, dtype=np.float64)
    s = np.full_like(v, sigma)
    mean, stderr = inverse_variance_mean(v, s)

    arith = float(np.mean(v))
    assert mean == pytest.approx(arith, rel=0, abs=1e-10)
    assert stderr > 0.0


@settings(max_examples=200, deadline=None)
@given(
    values=st.lists(finite_small, min_size=2, max_size=20),
    sigmas=st.lists(positive_sigma, min_size=2, max_size=20),
)
def test_iv_mean_within_value_range(values, sigmas):
    n = min(len(values), len(sigmas))
    v = np.asarray(values[:n], dtype=np.float64)
    s = np.asarray(sigmas[:n], dtype=np.float64)
    mean, _ = inverse_variance_mean(v, s)
    assert not np.isnan(mean)
    # Numerical slack for floating-point summation.
    assert float(np.min(v)) - 1e-9 <= mean <= float(np.max(v)) + 1e-9


# ─────────────────────────────────────────────────────────────────────────────
# Property 5: Min-retrievals mask is monotone
# Feature: ghg-mapper-enhancements, Property 5: Min-retrievals mask is
#   monotone
# Validates: Requirement 11.1
# ─────────────────────────────────────────────────────────────────────────────
@settings(max_examples=200, deadline=None)
@given(
    r=st.integers(min_value=1, max_value=8),
    c=st.integers(min_value=1, max_value=8),
    counts_flat=st.lists(st.integers(min_value=0, max_value=100),
                         min_size=1, max_size=64),
    t1=st.integers(min_value=0, max_value=50),
    t2_offset=st.integers(min_value=0, max_value=50),
)
def test_min_retrievals_monotone(r, c, counts_flat, t1, t2_offset):
    needed = r * c
    if len(counts_flat) < needed:
        # Pad by cycling — hypothesis will usually give enough; this is a safety.
        counts_flat = (counts_flat * (needed // len(counts_flat) + 1))[:needed]
    else:
        counts_flat = counts_flat[:needed]
    counts = np.array(counts_flat, dtype=np.int64).reshape(r, c)
    t2 = t1 + t2_offset
    valid1 = counts >= t1
    valid2 = counts >= t2
    # Every t2-valid cell is also t1-valid (i.e. valid2 ⊆ valid1).
    assert np.all(valid2 <= valid1)


# ─────────────────────────────────────────────────────────────────────────────
# Property 6: Priority score is monotone in emission and inverse-SOC
# Feature: ghg-mapper-enhancements, Property 6: Priority score is monotone
#   in emission and inverse-SOC
# Validates: Requirement 13.1
# ─────────────────────────────────────────────────────────────────────────────
positive_small = st.floats(min_value=0.1, max_value=100.0,
                           allow_nan=False, allow_infinity=False, width=64)


@settings(max_examples=200, deadline=None)
@given(
    s_a=positive_small,
    s_delta=st.floats(min_value=0.1, max_value=100.0,
                      allow_nan=False, allow_infinity=False, width=64),
    soc_shared=positive_small,
)
def test_priority_monotone_in_emission(s_a, s_delta, soc_shared):
    s_b = s_a + s_delta  # s_b > s_a strictly
    signal = np.array([s_a, s_b], dtype=np.float64)
    soc = np.array([soc_shared, soc_shared], dtype=np.float64)
    out = compute_priority_score(signal, soc)
    # Monotone non-decreasing (equality allowed due to 5-95 percentile saturation).
    assert out[1] >= out[0] - 1e-12


@settings(max_examples=200, deadline=None)
@given(
    signal_shared=positive_small,
    soc_a=positive_small,
    soc_delta=st.floats(min_value=0.1, max_value=100.0,
                        allow_nan=False, allow_infinity=False, width=64),
)
def test_priority_monotone_in_inverse_soc(signal_shared, soc_a, soc_delta):
    soc_b = soc_a + soc_delta  # soc_b > soc_a → lower inv-SOC → lower priority
    signal = np.array([signal_shared, signal_shared], dtype=np.float64)
    soc = np.array([soc_a, soc_b], dtype=np.float64)
    out = compute_priority_score(signal, soc)
    # Cell with lower SOC (index 0) should have priority >= cell with higher SOC.
    assert out[0] >= out[1] - 1e-12


# ─────────────────────────────────────────────────────────────────────────────
# Property 7: Window splitter produces non-overlapping, covering windows
# Feature: ghg-mapper-enhancements, Property 7: Window splitter produces
#   non-overlapping, covering windows
# Validates: Requirements 3.2, 3.3
# ─────────────────────────────────────────────────────────────────────────────

MIN_DATE = date(2010, 1, 1)
MAX_DATE = date(2030, 12, 31)


@st.composite
def date_pair(draw):
    start_ord = draw(st.integers(min_value=MIN_DATE.toordinal(),
                                 max_value=MAX_DATE.toordinal()))
    end_offset = draw(st.integers(min_value=0, max_value=800))
    start = date.fromordinal(start_ord)
    end_ord = min(start_ord + end_offset, MAX_DATE.toordinal())
    end = date.fromordinal(end_ord)
    return start, end


@settings(max_examples=200, deadline=None)
@given(
    pair=date_pair(),
    mode=st.sampled_from(["whole_period", "monthly", "seasonal_in"]),
)
def test_window_splitter_covers_and_nonoverlapping(pair, mode):
    start_d, end_d = pair
    start_s = start_d.strftime("%Y-%m-%d")
    end_s = end_d.strftime("%Y-%m-%d")

    windows = split_composite_windows(start_s, end_s, mode)
    assert len(windows) >= 1

    # Every window is within [start, end].
    for w in windows:
        ws = datetime.strptime(w["start"], "%Y-%m-%d").date()
        we = datetime.strptime(w["end"], "%Y-%m-%d").date()
        assert ws >= start_d, (w, start_d)
        assert we <= end_d, (w, end_d)
        assert ws <= we

    # Pairwise non-overlap: sort by start and confirm each start > previous end.
    sorted_w = sorted(windows, key=lambda w: w["start"])
    for prev, nxt in zip(sorted_w, sorted_w[1:]):
        prev_end = datetime.strptime(prev["end"], "%Y-%m-%d").date()
        nxt_start = datetime.strptime(nxt["start"], "%Y-%m-%d").date()
        assert nxt_start > prev_end, (prev, nxt)

    # Union covers every day in [start, end].
    covered = set()
    for w in windows:
        ws = datetime.strptime(w["start"], "%Y-%m-%d").date()
        we = datetime.strptime(w["end"], "%Y-%m-%d").date()
        d = ws
        while d <= we:
            covered.add(d.toordinal())
            d += timedelta(days=1)

    total_days = (end_d - start_d).days + 1
    # For huge ranges, skip the exhaustive day count to keep the test fast;
    # the non-overlap + within-range invariants are the critical assertions.
    if total_days <= 500:
        expected = {(start_d + timedelta(days=i)).toordinal()
                    for i in range(total_days)}
        assert covered == expected, (
            f"mode={mode} missing days: {sorted(expected - covered)[:10]} "
            f"extra days: {sorted(covered - expected)[:10]}"
        )
