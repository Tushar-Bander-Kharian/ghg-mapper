"""Property-based tests for the ghg-mapper-fixes spec.

Feature: ghg-mapper-fixes
"""

import math

import numpy as np
import pytest
from hypothesis import given, settings, strategies as st

from ghg_mapper.pipeline.run_pipeline import (
    _buffer_bbox,
    _filter_lat_lon_in_bbox,
    _build_cmr_params,
)


# Strategies for lat/lon/bboxes. Use bounded floats to avoid NaN/inf; the
# production code never receives those from the dialog (QDoubleSpinBox ranges).
lon_strategy = st.floats(min_value=-180.0, max_value=180.0,
                         allow_nan=False, allow_infinity=False, width=32)
lat_strategy = st.floats(min_value=-90.0,  max_value=90.0,
                         allow_nan=False, allow_infinity=False, width=32)
buffer_strategy = st.floats(min_value=0.0, max_value=10.0,
                            allow_nan=False, allow_infinity=False, width=32)


@st.composite
def valid_bbox(draw):
    """Draw a well-formed [west, south, east, north] with west<=east, south<=north."""
    w = draw(lon_strategy)
    e = draw(st.floats(min_value=w, max_value=180.0, allow_nan=False,
                       allow_infinity=False, width=32))
    s = draw(lat_strategy)
    n = draw(st.floats(min_value=s, max_value=90.0, allow_nan=False,
                       allow_infinity=False, width=32))
    return [w, s, e, n]


# -------------------------------------------------------------------------
# Property 1: Buffered bbox computation is correct
# Feature: ghg-mapper-fixes, Property 1: Buffered bbox computation is correct
# Validates: Requirements 1.1
# -------------------------------------------------------------------------
@settings(max_examples=200)
@given(bbox=valid_bbox(), buf=buffer_strategy)
def test_buffered_bbox_expands_and_clamps(bbox, buf):
    w, s, e, n = bbox
    out = _buffer_bbox(bbox, buf)
    ow, os_, oe, on = out

    # Clamped to valid geographic ranges
    assert -180.0 <= ow <= 180.0
    assert  -90.0 <= os_ <= 90.0
    assert -180.0 <= oe <= 180.0
    assert  -90.0 <= on <= 90.0

    # Each side expanded by exactly the buffer, subject to clamping
    assert ow == pytest.approx(max(w - buf, -180.0))
    assert os_ == pytest.approx(max(s - buf,  -90.0))
    assert oe == pytest.approx(min(e + buf,  180.0))
    assert on == pytest.approx(min(n + buf,   90.0))

    # Buffered bbox never shrinks the input (for non-negative buffer)
    assert ow <= w
    assert os_ <= s
    assert oe >= e
    assert on >= n


# -------------------------------------------------------------------------
# Property 2: Original bbox filter excludes out-of-bounds points
# Feature: ghg-mapper-fixes, Property 2: Original bbox filter excludes
#     out-of-bounds points
# Validates: Requirements 1.3
# -------------------------------------------------------------------------
@settings(max_examples=200)
@given(
    bbox=valid_bbox(),
    lats=st.lists(lat_strategy, min_size=0, max_size=50),
    lons=st.lists(lon_strategy, min_size=0, max_size=50),
    buf=buffer_strategy,  # irrelevant to the filter output; proves independence
)
def test_filter_only_keeps_points_inside_original_bbox(bbox, lats, lons, buf):
    # The spec says: regardless of the buffer used for CMR, the downstream
    # lat/lon mask uses the ORIGINAL (un-buffered) bbox.
    n = min(len(lats), len(lons))
    if n == 0:
        return
    lat_arr = np.array(lats[:n], dtype=np.float64)
    lon_arr = np.array(lons[:n], dtype=np.float64)

    mask = _filter_lat_lon_in_bbox(lat_arr, lon_arr, bbox)
    assert mask.dtype == np.bool_
    assert mask.shape == lat_arr.shape

    w, s, e, nn = bbox
    # Every kept point is inside the original bbox
    kept_lat = lat_arr[mask]
    kept_lon = lon_arr[mask]
    assert np.all(kept_lat >= s)
    assert np.all(kept_lat <= nn)
    assert np.all(kept_lon >= w)
    assert np.all(kept_lon <= e)

    # Every dropped point is outside the original bbox
    dropped_lat = lat_arr[~mask]
    dropped_lon = lon_arr[~mask]
    outside = (
        (dropped_lat < s) | (dropped_lat > nn) |
        (dropped_lon < w) | (dropped_lon > e)
    )
    assert np.all(outside)

    # Buffer value is independent of the filter (sanity: compute buffered
    # bbox, feed the SAME lats/lons to the filter with buffered bbox and
    # confirm the original-bbox filter is stricter-or-equal).
    buffered = _buffer_bbox(bbox, buf)
    mask_buffered = _filter_lat_lon_in_bbox(lat_arr, lon_arr, buffered)
    # Original mask must be a subset of buffered mask.
    assert np.all(mask <= mask_buffered)


# -------------------------------------------------------------------------
# Property 3: Empty version string omits version parameter
# Feature: ghg-mapper-fixes, Property 3: Empty version string omits version
#     parameter
# Validates: Requirements 2.4
# -------------------------------------------------------------------------
empty_version_strategy = st.one_of(
    st.none(),
    st.just(""),
    st.from_regex(r"^[ \t\r\n]{1,8}$", fullmatch=True),
)
non_empty_version_strategy = (
    st.text(alphabet=st.characters(blacklist_categories=("Cs",),
                                    blacklist_characters="\x00"),
            min_size=1, max_size=10)
    .filter(lambda s: s.strip() != "")
)


@settings(max_examples=200)
@given(
    short_name=st.text(alphabet=st.characters(whitelist_categories=("Lu", "Ll", "Nd"), whitelist_characters="_"),
                       min_size=1, max_size=20),
    version=empty_version_strategy,
    start=st.just("2023-01-01"),
    end=st.just("2023-12-31"),
    bbox=valid_bbox(),
    max_granules=st.integers(min_value=1, max_value=100),
)
def test_empty_version_omits_param(short_name, version, start, end, bbox, max_granules):
    params = _build_cmr_params(short_name, version, start, end, bbox, max_granules)
    assert "version" not in params


@settings(max_examples=200)
@given(
    short_name=st.text(alphabet=st.characters(whitelist_categories=("Lu", "Ll", "Nd"), whitelist_characters="_"),
                       min_size=1, max_size=20),
    version=non_empty_version_strategy,
    start=st.just("2023-01-01"),
    end=st.just("2023-12-31"),
    bbox=valid_bbox(),
    max_granules=st.integers(min_value=1, max_value=100),
)
def test_non_empty_version_includes_param(short_name, version, start, end, bbox, max_granules):
    params = _build_cmr_params(short_name, version, start, end, bbox, max_granules)
    assert params.get("version") == version
