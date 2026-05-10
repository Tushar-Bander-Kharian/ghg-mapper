"""
methodology.py — Pure numpy helpers for the GHG Mapper enhancement pipeline.

This module is intentionally dependency-light: only ``numpy`` is required;
``scipy`` is used opportunistically (``scipy.ndimage`` for percentile filters,
``scipy.stats`` for Gaussian CDF) with pure-Python fallbacks. It performs no
I/O, does not import any GEE or QGIS/PyQt symbols, and every function is
safe to call from a standard Python interpreter or test harness.

All array arguments are ``numpy.ndarray`` (or array-like convertible via
``np.asarray``). Floats may be NaN to represent "no data"; helpers propagate
NaN conservatively unless documented otherwise.

Feature: ghg-mapper-enhancements
"""

from __future__ import annotations

import math
from datetime import date, datetime, timedelta
from typing import List, Optional, Sequence, Tuple

import numpy as np

# Optional scipy — degrade gracefully when absent.
try:
    from scipy.ndimage import generic_filter as _generic_filter  # type: ignore
    _HAS_SCIPY_NDIMAGE = True
except Exception:  # pragma: no cover - scipy is a declared dependency
    _HAS_SCIPY_NDIMAGE = False

try:
    from scipy.stats import norm as _scipy_norm  # type: ignore
    _HAS_SCIPY_STATS = True
except Exception:  # pragma: no cover
    _HAS_SCIPY_STATS = False


# ─────────────────────────────────────────────────────────────────────────────
# 1. Enhancement above local background
# ─────────────────────────────────────────────────────────────────────────────

def _odd(n: int) -> int:
    """Round ``n`` up to the nearest odd integer ≥ 1."""
    n = max(1, int(n))
    if n % 2 == 0:
        n += 1
    return n


def compute_enhancement(
    conc: np.ndarray,
    window_cells: int,
    background_percentile: float = 10.0,
) -> Tuple[np.ndarray, np.ndarray]:
    """Return ``(enhancement, background)`` arrays for a 2-D concentration grid.

    Parameters
    ----------
    conc : np.ndarray, shape (R, C)
        2-D concentration array (ppb for CH₄, ppm for XCO₂). May contain NaN.
    window_cells : int
        Side length of the square neighborhood (in cells) centered on each
        output cell. Rounded up to the nearest odd integer so the window has
        a well-defined center.
    background_percentile : float, default 10.0
        Percentile (0–100) used to estimate the local background from the
        neighborhood. 10 ≈ "regional clean air" for CH₄ / XCO₂.

    Returns
    -------
    enhancement : np.ndarray, shape (R, C)
        ``conc - background`` elementwise. NaN where ``conc`` is NaN or where
        the neighborhood contains no valid (non-NaN) pixels.
    background : np.ndarray, shape (R, C)
        The per-cell nan-aware percentile over the neighborhood. NaN where the
        neighborhood is entirely NaN.

    Notes
    -----
    * Borders are handled with ``mode='nearest'`` (scipy) or explicit window
      clipping (fallback path), so edge cells are computed from the valid
      portion of their (possibly partial) window.
    * Uses ``scipy.ndimage.generic_filter`` with a nan-aware callback when
      scipy is available; otherwise falls back to a pure-numpy loop.
    """
    arr = np.asarray(conc, dtype=np.float64)
    if arr.ndim != 2:
        raise ValueError(f"compute_enhancement: expected 2-D array, got shape {arr.shape}")

    w = _odd(window_cells)
    pct = float(background_percentile)

    def _nanpct(values: np.ndarray) -> float:
        # Percentile over a flat window; all-NaN → NaN.
        if np.all(np.isnan(values)):
            return np.nan
        return float(np.nanpercentile(values, pct))

    if _HAS_SCIPY_NDIMAGE:
        background = _generic_filter(
            arr, _nanpct, size=w, mode="nearest"
        )
    else:
        R, C = arr.shape
        half = w // 2
        background = np.full_like(arr, np.nan, dtype=np.float64)
        for i in range(R):
            i0 = max(0, i - half)
            i1 = min(R, i + half + 1)
            for j in range(C):
                j0 = max(0, j - half)
                j1 = min(C, j + half + 1)
                background[i, j] = _nanpct(arr[i0:i1, j0:j1].ravel())

    enhancement = arr - background
    # NaN where either input or background is NaN.
    nan_mask = np.isnan(arr) | np.isnan(background)
    enhancement = np.where(nan_mask, np.nan, enhancement)
    return enhancement, background


# ─────────────────────────────────────────────────────────────────────────────
# 2. Inverse-variance weighted mean (scalar and gridded)
# ─────────────────────────────────────────────────────────────────────────────

def inverse_variance_mean(
    values: np.ndarray,
    uncertainties: np.ndarray,
) -> Tuple[float, float]:
    """Return ``(weighted_mean, stderr)`` for 1-D paired values/uncertainties.

    * Drops NaN values / uncertainties and non-finite uncertainties.
    * If any σ == 0 after filtering, falls back to the arithmetic mean of the
      valid values and returns ``stderr = 0.0``.
    * Empty input (after filtering) → ``(nan, nan)``.
    * Otherwise: ``mean = Σ(vᵢ / σᵢ²) / Σ(1 / σᵢ²)`` and
      ``stderr = 1 / sqrt(Σ(1 / σᵢ²))``.
    """
    v = np.asarray(values, dtype=np.float64).ravel()
    s = np.asarray(uncertainties, dtype=np.float64).ravel()
    if v.size == 0 or s.size == 0:
        return (float("nan"), float("nan"))
    n = min(v.size, s.size)
    v = v[:n]
    s = s[:n]

    finite = np.isfinite(v) & np.isfinite(s) & (s >= 0.0)
    v = v[finite]
    s = s[finite]
    if v.size == 0:
        return (float("nan"), float("nan"))

    if np.any(s == 0.0):
        # Treat σ=0 as "perfectly known" — arithmetic mean is the limiting
        # behavior; stderr collapses to 0.
        return (float(np.mean(v)), 0.0)

    w = 1.0 / (s * s)
    sum_w = float(np.sum(w))
    mean = float(np.sum(v * w) / sum_w)
    stderr = float(1.0 / math.sqrt(sum_w))
    return (mean, stderr)


def grid_inverse_variance(
    lats: np.ndarray,
    lons: np.ndarray,
    values: np.ndarray,
    uncertainties: np.ndarray,
    bbox: Sequence[float],
    grid_res: float,
    min_retrievals: int = 5,
) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Bin sparse retrievals onto a regular grid via inverse-variance weighting.

    Parameters
    ----------
    lats, lons : np.ndarray, shape (N,)
        Retrieval coordinates (degrees).
    values : np.ndarray, shape (N,)
        Retrieval values (e.g. XCO₂ in ppm).
    uncertainties : np.ndarray, shape (N,)
        Per-retrieval 1-σ uncertainty. Must match the unit of ``values``.
    bbox : [west, south, east, north]
        Geographic extent of the grid, degrees.
    grid_res : float
        Cell size in degrees.
    min_retrievals : int, default 5
        Cells with a retrieval count strictly below this threshold are set
        to NaN in ``mean_grid``. ``count_grid`` and ``stderr_grid`` always
        reflect the underlying counts/weights.

    Returns
    -------
    mean_grid : np.ndarray, shape (rows, cols)  [float64]
    stderr_grid : np.ndarray, shape (rows, cols)  [float64]
    count_grid : np.ndarray, shape (rows, cols)  [int64]

    Notes
    -----
    * Grid is indexed ``[row, col]`` with row 0 at the north edge (top-down).
    * Empty input returns all-NaN / zero-count grids of the correct shape.
    * Points outside the bbox are silently dropped.
    * σ ≤ 0 or NaN inputs are dropped per point.
    """
    west, south, east, north = [float(x) for x in bbox]
    if grid_res <= 0:
        raise ValueError("grid_res must be > 0")

    cols = max(1, int(math.ceil((east - west) / grid_res)))
    rows = max(1, int(math.ceil((north - south) / grid_res)))

    sum_vw = np.zeros((rows, cols), dtype=np.float64)
    sum_w = np.zeros((rows, cols), dtype=np.float64)
    count = np.zeros((rows, cols), dtype=np.int64)

    lat = np.asarray(lats, dtype=np.float64).ravel()
    lon = np.asarray(lons, dtype=np.float64).ravel()
    val = np.asarray(values, dtype=np.float64).ravel()
    unc = np.asarray(uncertainties, dtype=np.float64).ravel()
    n = min(lat.size, lon.size, val.size, unc.size)
    if n == 0:
        mean_grid = np.full((rows, cols), np.nan, dtype=np.float64)
        stderr_grid = np.full((rows, cols), np.nan, dtype=np.float64)
        return mean_grid, stderr_grid, count

    lat = lat[:n]; lon = lon[:n]; val = val[:n]; unc = unc[:n]

    good = (
        np.isfinite(lat) & np.isfinite(lon) &
        np.isfinite(val) & np.isfinite(unc) & (unc > 0.0) &
        (lat >= south) & (lat <= north) &
        (lon >= west) & (lon <= east)
    )
    lat = lat[good]; lon = lon[good]; val = val[good]; unc = unc[good]

    if lat.size:
        col_ix = np.floor((lon - west) / grid_res).astype(np.int64)
        row_ix = np.floor((north - lat) / grid_res).astype(np.int64)
        np.clip(col_ix, 0, cols - 1, out=col_ix)
        np.clip(row_ix, 0, rows - 1, out=row_ix)

        w = 1.0 / (unc * unc)
        np.add.at(sum_vw, (row_ix, col_ix), val * w)
        np.add.at(sum_w, (row_ix, col_ix), w)
        np.add.at(count, (row_ix, col_ix), 1)

    with np.errstate(divide="ignore", invalid="ignore"):
        mean_grid = np.where(sum_w > 0, sum_vw / sum_w, np.nan)
        stderr_grid = np.where(sum_w > 0, 1.0 / np.sqrt(sum_w), np.nan)

    if min_retrievals > 0:
        mean_grid = np.where(count >= min_retrievals, mean_grid, np.nan)

    return mean_grid, stderr_grid, count


# ─────────────────────────────────────────────────────────────────────────────
# 3. Mass-balance flux
# ─────────────────────────────────────────────────────────────────────────────

# Dry-air column molar density (mol/m²). 2.12e22 is the commonly-cited value
# (e.g. Jacob et al. 2016, Buchwitz et al. 2017) for a total atmospheric column.
_DRY_AIR_COLUMN_MOL_M2 = 2.12e22


def mass_balance_flux(
    enhancement: np.ndarray,
    wind_speed_ms: np.ndarray,
    grid_length_m: float,
    molecular_mass_g_mol: float,
    species: str = "ch4",
) -> np.ndarray:
    """First-order mass-balance flux estimate in kg/ha/day.

    Simplification of the column-flux equation::

        flux[kg/m²/s] = ΔX × M × c × u / L

    where ``ΔX`` is the mole-fraction enhancement (dimensionless), ``M`` is
    the tracer molecular mass in kg/mol, ``c`` is the dry-air column number
    density (2.12e22 mol/m²), ``u`` is wind speed (m/s), and ``L`` is the
    grid length (m).

    Parameters
    ----------
    enhancement : array-like
        Enhancement in ppb (species='ch4') or ppm (species='xco2').
    wind_speed_ms : array-like
        Wind speed magnitude in m/s.
    grid_length_m : float
        Grid cell length in meters.
    molecular_mass_g_mol : float
        Molecular mass of the tracer (g/mol). 16.04 for CH₄, 44.01 for CO₂.
    species : {'ch4', 'xco2'}
        Selects the unit scaling for ``enhancement``: CH₄ is ppb (×1e-9),
        XCO₂ is ppm (×1e-6).

    Returns
    -------
    np.ndarray
        Flux in kg/ha/day. NaN propagates from either input.
    """
    enh = np.asarray(enhancement, dtype=np.float64)
    u = np.asarray(wind_speed_ms, dtype=np.float64)

    sp = species.lower()
    if sp == "ch4":
        dx = enh * 1e-9
    elif sp in ("xco2", "co2"):
        dx = enh * 1e-6
    else:
        raise ValueError(f"Unknown species {species!r}; expected 'ch4' or 'xco2'")

    if grid_length_m <= 0:
        raise ValueError("grid_length_m must be > 0")

    M_kg_mol = float(molecular_mass_g_mol) / 1000.0
    c = _DRY_AIR_COLUMN_MOL_M2

    # kg / m² / s
    flux_kg_m2_s = dx * M_kg_mol * c * u / float(grid_length_m)
    # kg / ha / day  (1 ha = 1e4 m²; 1 day = 86400 s)
    flux_kg_ha_day = flux_kg_m2_s * 1.0e4 * 86400.0
    return flux_kg_ha_day


# ─────────────────────────────────────────────────────────────────────────────
# 4. Cropland / generic boolean masking
# ─────────────────────────────────────────────────────────────────────────────

def apply_mask(data: np.ndarray, mask: np.ndarray) -> np.ndarray:
    """Return ``data`` with cells where ``mask == 0`` replaced by NaN.

    * ``mask`` may be boolean or integer; any non-zero value is "keep".
    * NaN in ``data`` is preserved regardless of ``mask`` value.
    * Input ``data`` is not modified in place; a new ``float64`` array is
      returned (ints are promoted because NaN requires a float dtype).
    """
    d = np.asarray(data, dtype=np.float64)
    m = np.asarray(mask)
    if d.shape != m.shape:
        raise ValueError(
            f"apply_mask: shape mismatch data={d.shape} mask={m.shape}"
        )
    keep = m.astype(bool)
    return np.where(keep, d, np.nan)


# ─────────────────────────────────────────────────────────────────────────────
# 5. Inverse-distance weighting (IDW) onto a regular grid
# ─────────────────────────────────────────────────────────────────────────────

def idw_interpolate(
    points_xy: np.ndarray,
    values: np.ndarray,
    grid_x: np.ndarray,
    grid_y: np.ndarray,
    power: float = 2.0,
) -> np.ndarray:
    """Inverse-distance weighted interpolation onto a regular (x, y) grid.

    Parameters
    ----------
    points_xy : np.ndarray, shape (N, 2)
        Source points as ``[[x, y], ...]`` (e.g. ``[[lon, lat], ...]``).
    values : np.ndarray, shape (N,)
        Source values.
    grid_x, grid_y : np.ndarray
        1-D coordinate arrays for the destination grid (lon, lat).
    power : float, default 2.0
        IDW power exponent.

    Returns
    -------
    np.ndarray, shape (len(grid_y), len(grid_x))
        Interpolated field. If a grid node exactly coincides with a source
        point, the returned value is the exact source value (no singular
        division).

    Notes
    -----
    * Empty inputs yield an all-NaN grid of the target shape.
    * Vectorized via broadcasting; memory scales as ``N * len(grid_x) * len(grid_y)``.
    """
    pts = np.asarray(points_xy, dtype=np.float64)
    vals = np.asarray(values, dtype=np.float64).ravel()
    gx = np.asarray(grid_x, dtype=np.float64).ravel()
    gy = np.asarray(grid_y, dtype=np.float64).ravel()

    out_shape = (gy.size, gx.size)
    if pts.size == 0 or vals.size == 0:
        return np.full(out_shape, np.nan, dtype=np.float64)
    if pts.ndim != 2 or pts.shape[1] != 2:
        raise ValueError(f"points_xy must have shape (N, 2); got {pts.shape}")
    if pts.shape[0] != vals.size:
        raise ValueError(
            f"points_xy ({pts.shape[0]}) and values ({vals.size}) length mismatch"
        )

    finite = np.isfinite(pts).all(axis=1) & np.isfinite(vals)
    pts = pts[finite]
    vals = vals[finite]
    if pts.shape[0] == 0:
        return np.full(out_shape, np.nan, dtype=np.float64)

    # Broadcast: gx shape (1, 1, C), gy shape (1, R, 1), pts shape (N, 1, 1)
    px = pts[:, 0][:, None, None]
    py = pts[:, 1][:, None, None]
    gxb = gx[None, None, :]
    gyb = gy[None, :, None]

    dx = gxb - px
    dy = gyb - py
    dist2 = dx * dx + dy * dy  # (N, R, C)

    # Exact-hit guard: where any source sits on a grid node, use that value.
    with np.errstate(divide="ignore"):
        weights = 1.0 / np.power(dist2, power / 2.0)

    # Identify exact hits per grid node (any source point distance == 0).
    exact_mask = dist2 == 0.0  # (N, R, C)
    has_exact = np.any(exact_mask, axis=0)  # (R, C)

    # Normal IDW: replace inf/nan weights for now; we'll override at exact hits.
    weights_safe = np.where(np.isfinite(weights), weights, 0.0)
    num = np.sum(weights_safe * vals[:, None, None], axis=0)
    den = np.sum(weights_safe, axis=0)
    with np.errstate(divide="ignore", invalid="ignore"):
        result = np.where(den > 0, num / den, np.nan)

    if np.any(has_exact):
        # For any exact hit, take the mean of the values whose dist==0.
        exact_vals = np.where(exact_mask, vals[:, None, None], np.nan)
        exact_mean = np.nanmean(exact_vals, axis=0)
        result = np.where(has_exact, exact_mean, result)

    return result


# ─────────────────────────────────────────────────────────────────────────────
# 6. Hotspot confidence (Gaussian)
# ─────────────────────────────────────────────────────────────────────────────

def hotspot_confidence_pct(
    enhancement: np.ndarray,
    stderr: np.ndarray,
    threshold: float,
) -> np.ndarray:
    """Return per-cell P(true enhancement > threshold), in percent [0, 100].

    Assumes a Gaussian measurement model with mean ``enhancement`` and
    standard deviation ``stderr``. Uses ``scipy.stats.norm.cdf`` if available,
    otherwise falls back to ``math.erf`` per element.

    Behavior
    --------
    * ``stderr == 0`` → 100 if ``enhancement > threshold`` else 0.
    * NaN in either input → NaN output.
    * ``stderr < 0`` is treated as NaN (undefined).
    """
    e = np.asarray(enhancement, dtype=np.float64)
    s = np.asarray(stderr, dtype=np.float64)
    if e.shape != s.shape:
        # Allow broadcasting up to the common shape.
        e, s = np.broadcast_arrays(e, s)
    thr = float(threshold)

    out = np.full(e.shape, np.nan, dtype=np.float64)
    nan_mask = np.isnan(e) | np.isnan(s) | (s < 0.0)

    # Zero-stderr branch: deterministic above/below threshold.
    zero_mask = (s == 0.0) & ~nan_mask
    out = np.where(zero_mask & (e > thr), 100.0, out)
    out = np.where(zero_mask & (e <= thr), 0.0, out)

    normal = ~nan_mask & ~zero_mask
    if np.any(normal):
        z = (e - thr) / s
        if _HAS_SCIPY_STATS:
            cdf = _scipy_norm.cdf(z)
        else:
            # erf-based fallback; math.erf is scalar only.
            flat_z = z.ravel()
            cdf_flat = np.empty_like(flat_z)
            sqrt2 = math.sqrt(2.0)
            for i, zi in enumerate(flat_z):
                cdf_flat[i] = 0.5 * (1.0 + math.erf(zi / sqrt2))
            cdf = cdf_flat.reshape(z.shape)
        out = np.where(normal, 100.0 * cdf, out)
    return out


# ─────────────────────────────────────────────────────────────────────────────
# 7. Priority score
# ─────────────────────────────────────────────────────────────────────────────

def _robust_minmax(x: np.ndarray) -> np.ndarray:
    """Normalize ``x`` to [0, 1] via 5th-95th percentile min-max, clipped.

    NaN propagates. Degenerate (``q95 == q5``) case returns zeros for
    non-NaN entries.
    """
    arr = np.asarray(x, dtype=np.float64)
    finite = np.isfinite(arr)
    if not np.any(finite):
        return np.full_like(arr, np.nan, dtype=np.float64)
    q5 = float(np.nanpercentile(arr, 5))
    q95 = float(np.nanpercentile(arr, 95))
    span = q95 - q5
    if span <= 0 or not math.isfinite(span):
        out = np.where(finite, 0.0, np.nan)
        return out
    scaled = (arr - q5) / span
    scaled = np.clip(scaled, 0.0, 1.0)
    scaled = np.where(np.isnan(arr), np.nan, scaled)
    return scaled


def compute_priority_score(
    signal: np.ndarray,
    soc_pct: np.ndarray,
) -> np.ndarray:
    """Composite priority score in [0, 1].

    ``priority = normalize(signal) × normalize(1 / max(soc_pct, 0.1))``

    where ``normalize`` is a 5th-95th percentile min-max rescale (clipped).
    Higher emission signal and lower SOC (more mitigation headroom) drive
    the score up. Either input NaN → NaN output.
    """
    sig = np.asarray(signal, dtype=np.float64)
    soc = np.asarray(soc_pct, dtype=np.float64)
    if sig.shape != soc.shape:
        sig, soc = np.broadcast_arrays(sig, soc)

    inv_soc = 1.0 / np.maximum(soc, 0.1)
    # Propagate NaN through the transformation even though max() would drop it.
    inv_soc = np.where(np.isnan(soc), np.nan, inv_soc)

    n_sig = _robust_minmax(sig)
    n_soc = _robust_minmax(inv_soc)
    out = n_sig * n_soc
    nan_mask = np.isnan(sig) | np.isnan(soc)
    out = np.where(nan_mask, np.nan, out)
    return out


# ─────────────────────────────────────────────────────────────────────────────
# 8. Temporal window splitter
# ─────────────────────────────────────────────────────────────────────────────

_DATE_FMT = "%Y-%m-%d"


def _parse(d: str) -> date:
    return datetime.strptime(d, _DATE_FMT).date()


def _fmt(d: date) -> str:
    return d.strftime(_DATE_FMT)


def _month_windows(start: date, end: date) -> List[dict]:
    """One window per calendar month intersecting [start, end]."""
    windows: List[dict] = []
    cur = date(start.year, start.month, 1)
    while cur <= end:
        # Compute the first day of the next month.
        if cur.month == 12:
            nxt = date(cur.year + 1, 1, 1)
        else:
            nxt = date(cur.year, cur.month + 1, 1)
        last_day = nxt - timedelta(days=1)
        w_start = max(cur, start)
        w_end = min(last_day, end)
        if w_end >= w_start:
            name = f"{cur.year:04d}-{cur.month:02d}_monthly"
            windows.append({"name": name, "start": _fmt(w_start), "end": _fmt(w_end)})
        cur = nxt
    return windows


def _seasonal_windows(start: date, end: date) -> List[dict]:
    """Kharif / Rabi / Zaid seasonal windows intersecting [start, end]."""
    # Each season is a (name, start_month, start_day, end_month, end_day) tuple.
    # Rabi spans Nov 1 → Mar 31 of the following calendar year (cross-year).
    windows: List[dict] = []

    years = list(range(start.year - 1, end.year + 2))
    segments: List[Tuple[str, date, date]] = []
    for y in years:
        # Kharif: Jun 1 – Oct 31, within year y
        segments.append((f"kharif_{y}", date(y, 6, 1), date(y, 10, 31)))
        # Zaid: Apr 1 – May 31, within year y
        segments.append((f"zaid_{y}", date(y, 4, 1), date(y, 5, 31)))
        # Rabi: Nov 1 (year y) – Mar 31 (year y+1)
        segments.append((f"rabi_{y}-{y+1}", date(y, 11, 1), date(y + 1, 3, 31)))

    for name, s, e in segments:
        clipped_start = max(s, start)
        clipped_end = min(e, end)
        if clipped_end >= clipped_start:
            windows.append({
                "name": name,
                "start": _fmt(clipped_start),
                "end": _fmt(clipped_end),
            })
    # Sort by start date (string order == chrono since ISO format).
    windows.sort(key=lambda w: w["start"])
    return windows


def split_composite_windows(
    start: str,
    end: str,
    mode: str,
    custom: Optional[List[dict]] = None,
) -> List[dict]:
    """Partition ``[start, end]`` into composite windows per ``mode``.

    Each returned dict has keys ``name`` (str), ``start`` (``YYYY-MM-DD``),
    ``end`` (``YYYY-MM-DD``). Windows are clipped to ``[start, end]``.

    Parameters
    ----------
    start, end : str
        ISO-8601 dates; ``start <= end``.
    mode : {'whole_period', 'monthly', 'seasonal_in', 'custom'}
    custom : list of dict, optional
        Required when ``mode == 'custom'``. Each entry must supply
        ``name``, ``start``, ``end``.

    Returns
    -------
    list of dict
        Sorted ascending by start date.
    """
    s = _parse(start)
    e = _parse(end)
    if e < s:
        raise ValueError(f"end ({end}) must be >= start ({start})")

    m = (mode or "whole_period").lower()
    if m == "whole_period":
        return [{"name": "whole_period", "start": _fmt(s), "end": _fmt(e)}]
    if m == "monthly":
        return _month_windows(s, e)
    if m == "seasonal_in":
        return _seasonal_windows(s, e)
    if m == "custom":
        if not custom:
            return []
        out: List[dict] = []
        for w in custom:
            if not isinstance(w, dict):
                raise ValueError(f"custom window must be a dict, got {type(w).__name__}")
            for k in ("name", "start", "end"):
                if k not in w:
                    raise ValueError(f"custom window missing key {k!r}: {w}")
            ws = _parse(w["start"])
            we = _parse(w["end"])
            if we < ws:
                raise ValueError(f"custom window {w['name']!r} has end < start")
            out.append({"name": str(w["name"]), "start": _fmt(ws), "end": _fmt(we)})
        out.sort(key=lambda x: x["start"])
        return out

    raise ValueError(
        f"Unknown compositing mode {mode!r}; expected one of "
        "whole_period | monthly | seasonal_in | custom"
    )


__all__ = [
    "compute_enhancement",
    "inverse_variance_mean",
    "grid_inverse_variance",
    "mass_balance_flux",
    "apply_mask",
    "idw_interpolate",
    "hotspot_confidence_pct",
    "compute_priority_score",
    "split_composite_windows",
]
