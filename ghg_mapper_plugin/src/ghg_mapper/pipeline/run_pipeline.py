"""
run_pipeline.py — GHG Mapper backend pipeline
===============================================
Multi-satellite GHG hotspot mapping.
Satellites: TROPOMI (CH4), OCO-2/OCO-3 (XCO2), GOSAT (XCO2+XCH4)
Ground truth: SOC/SIC field points, CAAQMS station CSV

Author : Tushar Bander <tushar.bander@amity.edu>
Version: 0.3.0
License: MIT

Emission factor sources:
  - IPCC 2019 Refinement to 2006 Guidelines, Vol 4 Agriculture
  - ICAR-NBSS&LUP soil carbon coefficients (national)
  - Walkley-Black correction: Nelson & Sommers (1982) x1.334
"""

from __future__ import annotations

import json
import logging
import os
import math
import traceback
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

import numpy as np

from . import methodology, stages_aux
from .methodology import split_composite_windows

log = logging.getLogger("ghg_mapper")

# ── constants ──────────────────────────────────────────────────────────────
WALKLEY_BLACK_CORRECTION = 1.334          # Nelson & Sommers 1982
HOTSPOT_PERCENTILE       = 90             # cells above this → hotspot flag
DEFAULT_GRID_DEG         = 0.1            # ~11 km grid

# GEE collection IDs
GEE_TROPOMI_CH4  = "COPERNICUS/S5P/OFFL/L3_CH4"
GEE_S5P_NO2      = "COPERNICUS/S5P/OFFL/L3_NO2"   # CAAQMS co-tracer validation

# CMR collections endpoint — used by :func:`discover_cmr_versions` to
# enumerate available dataset versions for OCO-2/3/GOSAT ACOS.
NASA_CMR_COLLECTIONS = "https://cmr.earthdata.nasa.gov/search/collections.json"

# NASA GES DISC — OCO-2/3 and GOSAT ACOS XCO2 (direct HTTPS download + h5py)
# Approach mirrors: https://github.com/sagarlimbu0/NASA-OCO2-OCO3
NASA_CMR_SEARCH   = "https://cmr.earthdata.nasa.gov/search/granules.json"
NASA_URS_HOST     = "urs.earthdata.nasa.gov"
OCO2_SHORT_NAME   = "OCO2_L2_Lite_FP"
OCO2_VERSION      = "11.2r"
OCO3_SHORT_NAME   = "OCO3_L2_Lite_FP"
OCO3_VERSION      = "10.4r"
# GOSAT ACOS L2 Lite — hosted on GES DISC, same EarthData auth as OCO-2/3.
# quality_flag variable is "quality_flag" (not "xco2_quality_flag").
GOSAT_SHORT_NAME  = "ACOS_L2_Lite_FP"
GOSAT_VERSION     = "9r"
# NIES GOSAT XCH4 — SFTP-only portal (prdct.gosat-2.nies.go.jp), separate credentials.
NIES_SFTP_HOST    = "prdct.gosat-2.nies.go.jp"


# ── config dataclass ───────────────────────────────────────────────────────
@dataclass
class PipelineConfig:
    start_date:    str                      # "YYYY-MM-DD"
    end_date:      str
    aoi_west:      float
    aoi_east:      float
    aoi_south:     float
    aoi_north:     float
    output_dir:    str
    gee_project:      str
    grid_res:         float = DEFAULT_GRID_DEG
    use_tropomi:      bool  = True
    use_oco2:         bool  = True
    use_oco3:         bool  = True
    use_gosat:        bool  = True
    earthdata_user:   Optional[str] = None   # NASA EarthData username  (urs.earthdata.nasa.gov)
    earthdata_pass:   Optional[str] = None   # NASA EarthData password
    nies_user:        Optional[str] = None   # NIES GOSAT portal username (prdct.gosat-2.nies.go.jp)
    nies_pass:        Optional[str] = None   # NIES GOSAT portal password
    soc_records:      List[dict] = field(default_factory=list)   # [{lat,lon,soc,sic}, ...]
    caaqms_csv:       Optional[str] = None
    wb_correction:    bool  = True
    cmr_spatial_buffer:   float = 2.0            # degrees to expand AOI bbox for CMR search
    oco2_version:         Optional[str] = "11.2r"  # OCO-2 dataset version for CMR (None = omit)
    oco3_version:         Optional[str] = "10.4r"  # OCO-3 dataset version for CMR (None = omit)
    gosat_version:        Optional[str] = "9r"     # GOSAT ACOS version for CMR (None = omit)

    # ── NEW: enhancement mode (Req 1) ──
    enhancement_mode:        bool  = False
    background_window_deg:   float = 1.5
    background_percentile:   int   = 10
    ch4_threshold_ppb:       float = 10.0
    xco2_threshold_ppm:      float = 1.5

    # ── NEW: cropland mask (Req 2) ──
    cropland_mask:           bool  = False
    include_grassland:       bool  = False

    # ── NEW: compositing (Req 3) ──
    compositing_mode:        str   = "whole_period"   # whole_period|monthly|seasonal_in|custom
    custom_windows:          List[dict] = field(default_factory=list)  # [{"name","start","end"}]

    # ── NEW: flux (Req 4) ──
    estimate_flux:           bool  = False

    # ── NEW: ERA5 (Req 5) ──
    use_era5:                bool  = False

    # ── NEW: FIRMS (Req 6) ──
    use_firms:               bool  = False
    firms_sensors:           str   = "both"   # viirs|modis|both

    # ── NEW: Livestock (Req 7) ──
    use_livestock:           bool  = False
    glw4_cattle_path:        Optional[str] = None
    glw4_buffalo_path:       Optional[str] = None
    glw4_goat_path:          Optional[str] = None
    glw4_sheep_path:         Optional[str] = None

    # ── NEW: NO2 co-tracer (Req 8) ──
    use_no2_cotracer:        bool  = False
    no2_high_percentile:     int   = 80
    no2_low_percentile:      int   = 40

    # ── NEW: Strict TROPOMI QA (Req 9) ──
    strict_tropomi_qa:       bool  = False
    tropomi_qa_threshold:    float = 0.5
    tropomi_albedo_threshold:float = 0.05
    tropomi_cloud_threshold: float = 0.3

    # ── NEW: Inverse-variance weighting (Req 10) — default ON (strict improvement) ──
    inverse_variance_weighting: bool = True

    # ── NEW: Minimum retrievals mask (Req 11) ──
    min_retrievals_per_cell: int   = 5

    # ── NEW: CAAQMS bias correction (Req 12) ──
    caaqms_bias_correction:  bool  = False
    idw_power:               float = 2.0

    # ── NEW: Priority score (Req 13) ──
    compute_priority:        bool  = False

    # ── NEW: Multi-scale fine grid (Req 14) ──
    multiscale_fine_grid:    bool  = False
    fine_grid_res:           float = 0.02

    @property
    def aoi_bounds(self):
        return [self.aoi_west, self.aoi_south, self.aoi_east, self.aoi_north]


# ── progress callback type ─────────────────────────────────────────────────
ProgressFn = Callable[[int, str], None]   # (percent, message)


# ═══════════════════════════════════════════════════════════════════════════
# Main entry point
# ═══════════════════════════════════════════════════════════════════════════

def run_full_pipeline(cfg: PipelineConfig,
                      progress_fn: Optional[ProgressFn] = None) -> dict:
    """
    Run the full GHG hotspot mapping pipeline.

    Returns a dict with output file paths and summary statistics.
    Calls progress_fn(percent, message) if provided.
    """
    def prog(pct: int, msg: str):
        log.info("[%3d%%] %s", pct, msg)
        if progress_fn:
            progress_fn(pct, msg)

    out = Path(cfg.output_dir)
    out.mkdir(parents=True, exist_ok=True)

    results = {
        "start_time": datetime.now().isoformat(),
        "config": asdict(cfg),
        "files": {},
        "stats": {},
        "errors": [],
    }

    try:
        # ── Step 1: initialise GEE ─────────────────────────────────────────
        prog(2, "Initialising Google Earth Engine …")
        ee = _init_gee(cfg.gee_project)
        aoi = ee.Geometry.BBox(cfg.aoi_west, cfg.aoi_south,
                               cfg.aoi_east,  cfg.aoi_north)

        # ── Step 2: partition the date range into composite windows ───────
        windows = split_composite_windows(
            cfg.start_date, cfg.end_date,
            cfg.compositing_mode,
            cfg.custom_windows or None,
        )
        if not windows:
            raise RuntimeError(
                f"Compositing mode {cfg.compositing_mode!r} produced no windows. "
                "Check cfg.custom_windows when using 'custom' mode."
            )
        prog(3, f"Compositing mode: {cfg.compositing_mode} — {len(windows)} window(s).")
        results["windows"] = windows  # include in run_config.json

        # ── Step 3: run per-window pipeline body ──────────────────────────
        for i, win in enumerate(windows):
            if win["name"] == "whole_period":
                win_dir = out
            else:
                win_dir = out / win["name"]
                win_dir.mkdir(parents=True, exist_ok=True)
            prog(5, f"Window {i+1}/{len(windows)}: {win['name']} "
                    f"({win['start']} → {win['end']})")
            win_result = _process_window(cfg, win, win_dir, ee, aoi, prog)
            # Merge per-window outputs into master results.
            results["files"].update(win_result.get("files", {}))
            if win_result.get("stats"):
                results["stats"][win["name"]] = win_result["stats"]
            results["errors"].extend(win_result.get("errors", []))

        # ── Step 4: write run metadata (once, at root) ────────────────────
        prog(95, "Writing run metadata …")
        summary_path = out / "run_summary.txt"
        config_path  = out / "run_config.json"
        _write_summary(cfg, results, summary_path)
        _write_config(cfg, results, config_path)
        results["files"]["run_summary"] = str(summary_path)
        results["files"]["run_config"]  = str(config_path)

        prog(100, "Pipeline complete ✓")
        results["end_time"] = datetime.now().isoformat()
        results["status"]   = "success"

    except Exception as exc:
        results["status"] = "failed"
        results["errors"].append(traceback.format_exc())
        prog(100, f"Pipeline failed: {exc}")
        raise

    return results


# ═══════════════════════════════════════════════════════════════════════════
# Per-window pipeline body
# ═══════════════════════════════════════════════════════════════════════════

def _process_window(cfg: "PipelineConfig",
                    window: dict,
                    win_dir: Path,
                    ee,
                    aoi,
                    prog: Optional[ProgressFn] = None) -> dict:
    """Run the per-window portion of the pipeline for one composite window.

    This is the main per-window orchestrator. It fetches every enabled
    composite into a ``composites`` dict of 2-D numpy arrays (design.md §4),
    derives enhancement / flux / priority grids via :mod:`methodology`, and
    writes the set of raster + vector outputs expected by the Results tab.

    Composites dict assembly order (mirrors design.md §2):
      1. Cropland mask        (if ``cfg.cropland_mask``)
      2. CH₄ composite        (TROPOMI L3 or L2-strict → numpy)
      3. XCO₂ composite       (OCO direct → numpy via inv-var or arithmetic)
      4. Enhancement          (if ``cfg.enhancement_mode``)
      5. ERA5 meteorology     (if ``cfg.use_era5``)
      6. TROPOMI NO₂          (if ``cfg.use_no2_cotracer``)
      7. FIRMS fires          (if ``cfg.use_firms``)
      8. Livestock densities  (if ``cfg.use_livestock``)
      9. Mass-balance flux    (if ``cfg.estimate_flux`` + enh + wind)
      10. Hotspot GeoPackage  (via :func:`_detect_hotspots_v2`)

    All stage failures are non-fatal: a missing stage drops keys from
    ``composites`` and columns from the hotspot GeoPackage, but the window
    continues. Unhandled exceptions are trapped at the end, logged to the
    per-window ``errors`` list, and the partial result is returned.

    Parameters
    ----------
    cfg : PipelineConfig
        Run configuration. ``cfg.start_date`` / ``cfg.end_date`` are ignored
        in favor of ``window['start']`` / ``window['end']``.
    window : dict
        Entry from :func:`methodology.split_composite_windows` with keys
        ``name``, ``start``, ``end``.
    win_dir : Path
        Output directory for this window (created by ``run_full_pipeline``).
    ee, aoi
        Initialised ``ee`` module handle and AOI geometry.
    prog : ProgressFn, optional
        Progress callback. Messages are prefixed with ``[<window_name>]``.

    Returns
    -------
    dict
        ``{"files": {...}, "stats": {...}, "errors": [...]}``. Never raises.
    """
    win_name = window["name"]
    win_start = window["start"]
    win_end = window["end"]

    def win_prog(pct: int, msg: str):
        tagged = f"[{win_name}] {msg}"
        if prog:
            prog(pct, tagged)
        else:
            log.info("[%3d%%] %s", pct, tagged)

    def _key(base: str) -> str:
        """Legacy file-key for whole_period; window-suffixed otherwise."""
        return base if win_name == "whole_period" else f"{base}_{win_name}"

    win_result: dict = {"files": {}, "stats": {}, "errors": []}
    composites: Dict[str, Any] = {}

    try:
        bbox = cfg.aoi_bounds

        # ── 1. Cropland mask (Req 2) ─────────────────────────────────────
        if cfg.cropland_mask:
            win_prog(8, "Fetching cropland mask (ESA WorldCover) …")
            crop = stages_aux.stage_cropland_mask(
                ee, aoi, bbox, cfg.grid_res,
                cfg.include_grassland, win_prog,
            )
            if crop is not None:
                composites["cropland_fraction"] = crop

        # ── 2. CH₄ composite (TROPOMI) ───────────────────────────────────
        win_prog(10, "Fetching TROPOMI CH₄ composite …")
        ch4_arr: Optional[np.ndarray] = None
        if cfg.use_tropomi:
            ch4_arr = _stage_tropomi_ch4(cfg, ee, aoi, bbox, win_start, win_end,
                                          prog=win_prog)
        if ch4_arr is None:
            win_prog(10, "⚠  TROPOMI: no CH₄ data — downstream CH₄ columns skipped.")
        else:
            composites["ch4_ppb"] = ch4_arr
            # TROPOMI L3 doesn't expose per-cell counts; we cannot satisfy
            # the min-retrievals mask the way we do for OCO. Skip ch4_count
            # so _detect_hotspots_v2 only applies the threshold to XCO₂.

        # Write the CH₄ raster from the numpy array (replaces the old
        # _export_image_to_drive_or_local call — we already have the array).
        if ch4_arr is not None:
            ch4_tif = _grid_array_to_tif(
                ch4_arr, bbox, cfg.grid_res,
                win_dir / "ch4_composite.tif", "CH4_ppb",
            )
            if ch4_tif is not None:
                win_result["files"][_key("ch4_composite")] = str(ch4_tif)

        # ── 3. XCO₂ composite (OCO/GOSAT via direct download) ────────────
        win_prog(25, "Fetching OCO-2 / OCO-3 / GOSAT XCO₂ (NASA GES DISC) …")
        xco2_result: Optional[Dict[str, Any]] = None
        if cfg.use_oco2 or cfg.use_oco3 or cfg.use_gosat:
            if cfg.earthdata_user and cfg.earthdata_pass:
                xco2_result = _stage_xco2_direct(
                    cfg,
                    win_start, win_end,
                    bbox, win_dir,
                    cfg.earthdata_user,
                    cfg.earthdata_pass,
                    use_oco2=cfg.use_oco2,
                    use_oco3=cfg.use_oco3,
                    use_gosat=cfg.use_gosat,
                    prog=win_prog,
                )
                if xco2_result is None:
                    win_prog(25, "⚠  OCO-2/3/GOSAT: no retrievals found or download failed.")
            else:
                win_prog(25, "⚠  OCO-2/3/GOSAT skipped — no EarthData credentials.")

        if xco2_result is not None:
            composites["xco2_ppm"] = xco2_result.get("mean_grid")
            if xco2_result.get("stderr_grid") is not None:
                composites["xco2_stderr"] = xco2_result["stderr_grid"]
            if xco2_result.get("count_grid") is not None:
                composites["xco2_count"] = xco2_result["count_grid"]

            tif_path = xco2_result.get("tif_path")
            if tif_path is not None and Path(tif_path).exists():
                win_result["files"][_key("xco2_composite")] = str(tif_path)
            stderr_tif = xco2_result.get("stderr_tif_path")
            if stderr_tif is not None and Path(stderr_tif).exists():
                win_result["files"][_key("xco2_composite_stderr")] = str(stderr_tif)
            fine_tif = xco2_result.get("fine_tif_path")
            if fine_tif is not None and Path(fine_tif).exists():
                win_result["files"][_key("xco2_composite_fine")] = str(fine_tif)

        # ── Optional legacy GOSAT XCH₄ via NIES (unchanged path) ─────────
        win_prog(30, "Fetching GOSAT XCH₄ (NIES portal) …")
        if cfg.use_gosat and cfg.nies_user and cfg.nies_pass:
            gosat_ch4_path = _stage_gosat_ch4_nies(
                win_start, win_end,
                bbox, win_dir,
                cfg.nies_user, cfg.nies_pass,
                cfg.grid_res, prog=win_prog,
            )
            if gosat_ch4_path is not None:
                win_result["files"][_key("gosat_ch4_composite")] = str(gosat_ch4_path)
            else:
                win_prog(30, "⚠  GOSAT XCH₄: download failed or no data in AOI.")
        elif cfg.use_gosat:
            win_prog(30, "⚠  GOSAT XCH₄ skipped — no NIES credentials.")

        # ── 3b. CAAQMS bias correction (Req 12) ──────────────────────────
        # Runs AFTER composites have ch4_ppb / xco2_ppm but BEFORE the
        # enhancement calc, so downstream stages see bias-corrected values.
        if (cfg.caaqms_bias_correction
                and cfg.caaqms_csv
                and Path(cfg.caaqms_csv).exists()):
            win_prog(48, "Applying CAAQMS bias correction …")
            bias_out = _apply_caaqms_bias_correction(
                cfg, cfg.caaqms_csv, composites, bbox, cfg.grid_res,
                win_dir, win_prog,
            )
            for key in ("ch4_bias_corrected_tif_path",
                        "xco2_bias_corrected_tif_path"):
                p = bias_out.get(key)
                if p:
                    base = key.replace("_tif_path", "")
                    win_result["files"][_key(base)] = str(p)

        # ── 4. Enhancement (Req 1) ───────────────────────────────────────
        if cfg.enhancement_mode:
            win_prog(45, "Computing enhancement above local background …")
            window_cells = max(1, int(round(cfg.background_window_deg / cfg.grid_res)))
            if composites.get("ch4_ppb") is not None:
                try:
                    enh, bg = methodology.compute_enhancement(
                        composites["ch4_ppb"], window_cells, cfg.background_percentile,
                    )
                    composites["ch4_enhancement"] = enh
                    composites["ch4_background"] = bg
                except Exception as e:
                    win_prog(45, f"⚠  CH₄ enhancement calc failed: {e}")
            if composites.get("xco2_ppm") is not None:
                try:
                    enh, bg = methodology.compute_enhancement(
                        composites["xco2_ppm"], window_cells, cfg.background_percentile,
                    )
                    composites["xco2_enhancement"] = enh
                    composites["xco2_background"] = bg
                except Exception as e:
                    win_prog(45, f"⚠  XCO₂ enhancement calc failed: {e}")

        # ── 5. ERA5 meteorology (Req 5) ──────────────────────────────────
        if cfg.use_era5:
            win_prog(55, "Fetching ERA5-Land meteorology …")
            era5 = stages_aux.stage_era5(
                ee, aoi, bbox, cfg.grid_res, win_start, win_end, win_prog,
            )
            if era5 is not None:
                for k in ("temp_c", "wind_speed_ms", "wind_dir_deg",
                          "precip_mm", "soil_moist"):
                    if era5.get(k) is not None:
                        composites[k] = era5[k]
                # Upwind source centroid (Req 5.3): 1 cell upwind along mean
                # wind direction. Meteorological direction is "where wind is
                # coming FROM" → upwind is AWAY from the current cell in the
                # direction the wind is coming from (i.e. the opposite of
                # advection). For source attribution we want the cell the
                # tracer most likely came from, which is AGAINST the wind
                # vector (u, v). dx = -sin(dir_rad), dy = -cos(dir_rad).
                wd = composites.get("wind_dir_deg")
                if wd is not None:
                    try:
                        rows, cols = wd.shape
                        ii, jj = np.meshgrid(
                            np.arange(rows), np.arange(cols), indexing="ij"
                        )
                        lat_c = (
                            cfg.aoi_bounds[3]
                            - (ii + 0.5) * cfg.grid_res
                        )
                        lon_c = (
                            cfg.aoi_bounds[0]
                            + (jj + 0.5) * cfg.grid_res
                        )
                        theta = np.deg2rad(wd)
                        # 1 cell upwind = - wind_vector * cell_size
                        du_lon = -np.sin(theta) * cfg.grid_res
                        du_lat = -np.cos(theta) * cfg.grid_res
                        composites["upwind_source_lat"] = (lat_c + du_lat).astype(np.float64)
                        composites["upwind_source_lon"] = (lon_c + du_lon).astype(np.float64)
                    except Exception as e:
                        win_prog(55, f"⚠  upwind source calc failed: {e}")

        # ── 6. NO₂ co-tracer (Req 8) ─────────────────────────────────────
        if cfg.use_no2_cotracer:
            win_prog(62, "Fetching TROPOMI NO₂ co-tracer …")
            no2 = stages_aux.stage_no2(
                ee, aoi, bbox, cfg.grid_res, win_start, win_end, win_prog,
            )
            if no2 is not None:
                composites["no2_column"] = no2
                # Source attribution (combustion / biogenic / ambiguous)
                ch4_for_src = composites.get("ch4_ppb")
                if ch4_for_src is not None:
                    try:
                        composites["source_type"] = _classify_source_type(
                            ch4_for_src, no2,
                            cfg.no2_high_percentile,
                            cfg.no2_low_percentile,
                        )
                    except Exception as e:
                        win_prog(62, f"⚠  NO₂ source classification failed: {e}")
                # Write NO₂ composite raster.
                no2_tif = _grid_array_to_tif(
                    no2, bbox, cfg.grid_res,
                    win_dir / "no2_composite.tif", "NO2_mol_m2",
                )
                if no2_tif is not None:
                    win_result["files"][_key("no2_composite")] = str(no2_tif)

        # ── 7. FIRMS fires (Req 6) ───────────────────────────────────────
        if cfg.use_firms:
            win_prog(68, "Fetching FIRMS active fires …")
            firms = stages_aux.stage_firms(
                ee, aoi, bbox, cfg.grid_res, win_start, win_end,
                cfg.firms_sensors, win_prog,
            )
            if firms is not None:
                for k in ("fire_count_viirs", "fire_count_modis", "fire_count_total"):
                    if firms.get(k) is not None:
                        composites[k] = firms[k]
                fires_gdf = firms.get("fires_gdf")
                if fires_gdf is not None and len(fires_gdf) > 0:
                    try:
                        fires_path = win_dir / "fires.gpkg"
                        fires_gdf.to_file(str(fires_path), driver="GPKG")
                        win_result["files"][_key("fires")] = str(fires_path)
                    except Exception as e:
                        win_prog(68, f"⚠  fires.gpkg write failed: {e}")

        # ── 8. Livestock (Req 7) ─────────────────────────────────────────
        if cfg.use_livestock:
            win_prog(72, "Loading FAO GLW4 livestock density …")
            glw4_paths = {
                "cattle":  cfg.glw4_cattle_path,
                "buffalo": cfg.glw4_buffalo_path,
                "goat":    cfg.glw4_goat_path,
                "sheep":   cfg.glw4_sheep_path,
            }
            lv = stages_aux.stage_livestock(
                bbox, cfg.grid_res, glw4_paths, win_prog,
            )
            if lv:
                for k, v in lv.items():
                    if v is not None:
                        composites[k] = v
                # Enteric CH₄ baseline (Req 7.5): cattle×56 + buffalo×55, /365.
                cattle = lv.get("cattle_density")
                buffalo = lv.get("buffalo_density")
                if cattle is not None or buffalo is not None:
                    # Need a grid shape to broadcast scalars.
                    ref = cattle if cattle is not None else buffalo
                    c_arr = (np.nan_to_num(cattle, nan=0.0)
                             if cattle is not None
                             else np.zeros_like(ref, dtype=np.float64))
                    b_arr = (np.nan_to_num(buffalo, nan=0.0)
                             if buffalo is not None
                             else np.zeros_like(ref, dtype=np.float64))
                    composites["enteric_ch4_ipcc_kg_ha_day"] = (
                        (c_arr * 56.0 + b_arr * 55.0) / 365.0
                    ).astype(np.float32)

        # ── 9. Mass-balance flux (Req 4) ─────────────────────────────────
        if cfg.estimate_flux:
            wind = composites.get("wind_speed_ms")
            if wind is None:
                win_prog(80, "⚠  Flux estimation skipped — ERA5 wind unavailable.")
            else:
                grid_length_m = cfg.grid_res * 111320.0
                if composites.get("ch4_enhancement") is not None:
                    try:
                        flux = methodology.mass_balance_flux(
                            composites["ch4_enhancement"], wind,
                            grid_length_m, 16.04, "ch4",
                        )
                        composites["ch4_flux_kg_ha_day"] = flux
                        t = _grid_array_to_tif(
                            flux, bbox, cfg.grid_res,
                            win_dir / "ch4_flux.tif", "CH4_flux_kg_ha_day",
                        )
                        if t is not None:
                            win_result["files"][_key("ch4_flux")] = str(t)
                    except Exception as e:
                        win_prog(80, f"⚠  CH₄ flux calc failed: {e}")
                if composites.get("xco2_enhancement") is not None:
                    try:
                        flux = methodology.mass_balance_flux(
                            composites["xco2_enhancement"], wind,
                            grid_length_m, 44.01, "xco2",
                        )
                        composites["xco2_flux_kg_ha_day"] = flux
                        t = _grid_array_to_tif(
                            flux, bbox, cfg.grid_res,
                            win_dir / "xco2_flux.tif", "XCO2_flux_kg_ha_day",
                        )
                        if t is not None:
                            win_result["files"][_key("xco2_flux")] = str(t)
                    except Exception as e:
                        win_prog(80, f"⚠  XCO₂ flux calc failed: {e}")

        # ── 9b. SOC × emission priority score (Req 13) ───────────────────
        if cfg.compute_priority and cfg.soc_records:
            win_prog(83, "Computing SOC × emission priority score …")
            try:
                soc_grid = _rasterize_soc_points(
                    cfg.soc_records, bbox, cfg.grid_res, cfg.idw_power,
                )
            except Exception as e:
                win_prog(83, f"⚠  SOC rasterization failed: {e}")
                soc_grid = None

            if soc_grid is not None:
                composites["soc_pct"] = soc_grid
                # Pick the best available emission signal for the priority
                # score: flux > enhancement > concentration (Req 13.2). The
                # numpy `or` operator is ambiguous on arrays, so we select
                # explicitly with `is not None` checks. When both CH4 and
                # XCO2 signals are present at the same tier, pick whichever
                # has more non-NaN cells.
                def _best_signal(keys_ch4: list, keys_xco2: list):
                    ch4_sig = None
                    xco2_sig = None
                    for k in keys_ch4:
                        v = composites.get(k)
                        if v is not None and isinstance(v, np.ndarray):
                            ch4_sig = v
                            break
                    for k in keys_xco2:
                        v = composites.get(k)
                        if v is not None and isinstance(v, np.ndarray):
                            xco2_sig = v
                            break
                    if ch4_sig is not None and xco2_sig is not None:
                        n_ch4 = int(np.sum(np.isfinite(ch4_sig)))
                        n_xco2 = int(np.sum(np.isfinite(xco2_sig)))
                        return ch4_sig if n_ch4 >= n_xco2 else xco2_sig
                    return ch4_sig if ch4_sig is not None else xco2_sig

                signal = _best_signal(
                    ["ch4_flux_kg_ha_day", "ch4_enhancement", "ch4_ppb"],
                    ["xco2_flux_kg_ha_day", "xco2_enhancement", "xco2_ppm"],
                )

                if signal is not None and signal.shape == soc_grid.shape:
                    try:
                        priority = methodology.compute_priority_score(
                            signal, soc_grid,
                        )
                        composites["priority_score"] = priority

                        # priority_rank: descending rank by priority_score.
                        # NaN cells are pushed to the back (highest rank #).
                        flat = priority.ravel()
                        finite_mask = np.isfinite(flat)
                        # argsort with NaN sinks them to the end naturally.
                        order = np.argsort(-np.where(finite_mask, flat, -np.inf),
                                           kind="stable")
                        ranks = np.empty(flat.size, dtype=np.int32)
                        ranks[order] = np.arange(flat.size, dtype=np.int32)
                        composites["priority_rank"] = ranks.reshape(priority.shape)

                        tif = _grid_array_to_tif(
                            priority, bbox, cfg.grid_res,
                            win_dir / "priority_map.tif", "priority_score",
                        )
                        if tif is not None:
                            win_result["files"][_key("priority_map")] = str(tif)
                    except Exception as e:
                        win_prog(83, f"⚠  Priority score calc failed: {e}")
                else:
                    win_prog(
                        83,
                        "⚠  Priority score skipped — no emission signal "
                        "available (flux / enhancement / concentration).",
                    )

        # ── 10. Hotspot GeoPackage (Req 1.5, 1.6, 2.4, 11.2, 15.4) ──────
        win_prog(85, "Detecting hotspots + writing GeoPackage …")
        hotspot_path = _detect_hotspots_v2(
            cfg, window, composites,
            win_dir / "ghg_hotspots.gpkg",
            win_prog,
        )
        if hotspot_path is not None:
            win_result["files"][_key("ghg_hotspots")] = str(hotspot_path)

        # ── SOC / SIC ground truth points (unchanged) ────────────────────
        win_prog(90, "Writing SOC/SIC ground truth points …")
        if cfg.soc_records:
            soc_path = _write_soc_points(cfg.soc_records,
                                         cfg.wb_correction,
                                         win_dir / "soc_points.gpkg")
            win_result["files"][_key("soc_points")] = str(soc_path)
            win_result["stats"]["soc_records"] = len(cfg.soc_records)

        # ── CAAQMS uncertainty analysis (unchanged) ──────────────────────
        win_prog(92, "Running CAAQMS uncertainty analysis …")
        if cfg.caaqms_csv and Path(cfg.caaqms_csv).exists():
            caaqms_out = win_dir / "caaqms_uncertainty.csv"
            caaqms_stats = _validate_caaqms(
                cfg.caaqms_csv, hotspot_path, caaqms_out
            )
            win_result["files"][_key("caaqms_uncertainty")] = str(caaqms_out)
            win_result["stats"]["caaqms"] = caaqms_stats

    except Exception as exc:
        win_result["errors"].append(traceback.format_exc())
        win_prog(100, f"Per-window pipeline error: {exc}")

    return win_result


def _classify_source_type(ch4: "np.ndarray",
                           no2: "np.ndarray",
                           no2_high_pct: int,
                           no2_low_pct: int) -> "np.ndarray":
    """Return a per-cell ``combustion | biogenic | ambiguous | ''`` object array.

    Classification (Req 8.2):
      * cell is a CH₄ hotspot AND NO₂ ≥ p(high) → ``combustion``
      * cell is a CH₄ hotspot AND NO₂ ≤ p(low)  → ``biogenic``
      * cell is a CH₄ hotspot otherwise          → ``ambiguous``
      * not a CH₄ hotspot                        → ``""``

    "CH₄ hotspot" here is the simple 90th-percentile test — we do not have
    access to the enhancement-mode threshold at this level. The caller
    uses this column descriptively; the authoritative hotspot flag still
    comes from :func:`_detect_hotspots_v2`.
    """
    if ch4.shape != no2.shape:
        raise ValueError(
            f"shape mismatch ch4={ch4.shape} no2={no2.shape}"
        )
    out = np.full(ch4.shape, "", dtype=object)
    finite_ch4 = np.isfinite(ch4)
    finite_no2 = np.isfinite(no2)

    if not finite_ch4.any() or not finite_no2.any():
        return out

    ch4_p90 = float(np.nanpercentile(ch4, HOTSPOT_PERCENTILE))
    no2_high = float(np.nanpercentile(no2, no2_high_pct))
    no2_low = float(np.nanpercentile(no2, no2_low_pct))

    is_hs = finite_ch4 & (ch4 >= ch4_p90)
    out = np.where(is_hs & finite_no2 & (no2 >= no2_high), "combustion", out)
    out = np.where(is_hs & finite_no2 & (no2 <= no2_low), "biogenic", out)
    # Ambiguous = hotspot but neither high nor low NO₂, OR NO₂ is NaN.
    out = np.where(is_hs & (out == ""), "ambiguous", out)
    return out


# ═══════════════════════════════════════════════════════════════════════════
# Stage functions
# ═══════════════════════════════════════════════════════════════════════════

def _init_gee(project_id: str):
    """Initialise and return the ee module."""
    import ee
    try:
        ee.Initialize(project=project_id)
    except Exception:
        ee.Authenticate()
        ee.Initialize(project=project_id)
    return ee


def _stage_tropomi_ch4(cfg: "PipelineConfig", ee, aoi, bbox, start: str, end: str,
                        prog: Optional[ProgressFn] = None) -> Optional["np.ndarray"]:
    """
    Build a mean CH₄ column composite from TROPOMI and return it as a 2-D
    numpy array (ppb) aligned to ``cfg.grid_res`` and ``cfg.aoi_bounds``.

    * When ``cfg.strict_tropomi_qa`` is True, the L2 product is used via
      :func:`stages_aux.stage_tropomi_ch4_strict` (QA + albedo + haze filters).
    * Otherwise the legacy L3 path applies the existing
      ``sensor_zenith_angle < 70°`` filter and downloads the composite to an
      array at ``cfg.grid_res``.

    Returns ``None`` when no imagery is found or the download fails — the
    caller is expected to treat this as "no CH₄ data available" and degrade
    gracefully (design §3, Req 19.1).
    """
    def _p(msg): prog(10, msg) if prog else log.info(msg)

    # ── Strict L2 path ────────────────────────────────────────────────
    if cfg.strict_tropomi_qa:
        return stages_aux.stage_tropomi_ch4_strict(
            ee, aoi, bbox, cfg.grid_res, start, end,
            cfg.tropomi_qa_threshold,
            cfg.tropomi_albedo_threshold,
            cfg.tropomi_cloud_threshold,
            prog,
        )

    # ── Legacy L3 path ────────────────────────────────────────────────
    # COPERNICUS/S5P/OFFL/L3_CH4 does NOT have a qa_value band.
    # Available quality proxy: sensor_zenith_angle (< 70° keeps good-geometry obs).
    col = (ee.ImageCollection(GEE_TROPOMI_CH4)
             .filterBounds(aoi)
             .filterDate(start, end)
             .map(lambda img: img.updateMask(
                 img.select("sensor_zenith_angle").lt(70)))
             .select("CH4_column_volume_mixing_ratio_dry_air"))

    n = col.size().getInfo()
    _p(f"TROPOMI: {n} image(s) found for the AOI / date range.")

    if n == 0:
        _p("⚠  TROPOMI collection is empty — no CH₄ data to composite.")
        return None

    composite = col.mean().rename("CH4_ppb").clip(aoi)
    arr = stages_aux._download_band_to_array(
        composite, aoi, bbox, cfg.grid_res, "CH4_ppb"
    )
    if arr is None:
        _p("⚠  TROPOMI: download to array failed — returning None.")
        return None
    return arr


# ── pure helpers (extracted for testability) ───────────────────────────────

def _buffer_bbox(bbox: list, buffer: float) -> list:
    """
    Expand a [west, south, east, north] bbox by ``buffer`` degrees on each
    side, clamped to valid geographic ranges (-180..180 lon, -90..90 lat).

    Pure function — no I/O, no mutation of inputs.
    """
    west, south, east, north = bbox
    return [
        max(west  - buffer, -180.0),
        max(south - buffer,  -90.0),
        min(east  + buffer,  180.0),
        min(north + buffer,   90.0),
    ]


def _filter_lat_lon_in_bbox(lat_arr, lon_arr, bbox):
    """
    Return a boolean numpy mask selecting points that lie inside the given
    ``[west, south, east, north]`` bbox using inclusive bounds
    (``south <= lat <= north`` and ``west <= lon <= east``).

    Lat/lon may be lists or numpy arrays; the output is a ``numpy.bool_`` mask
    with the same shape as ``lat_arr``. Pure function — no I/O.
    """
    import numpy as np
    lat = np.asarray(lat_arr)
    lon = np.asarray(lon_arr)
    west, south, east, north = bbox
    return (
        (lat >= south) & (lat <= north) &
        (lon >= west)  & (lon <= east)
    )


def _stage_xco2_direct(cfg: "PipelineConfig",
                        start: str, end: str, bbox: list, out_dir: Path,
                        earthdata_user: str, earthdata_pass: str,
                        use_oco2: bool = True, use_oco3: bool = True,
                        use_gosat: bool = True,
                        prog: Optional[ProgressFn] = None) -> Optional[Dict[str, Any]]:
    """
    Download OCO-2/OCO-3/GOSAT XCO₂ L2 Lite FP granules from NASA GES DISC
    via direct HTTPS and build a gridded XCO₂ composite.

    Returns
    -------
    dict with keys:
        "tif_path"         : Path to xco2_composite.tif (or None on failure)
        "mean_grid"        : 2-D float64 numpy array of XCO₂ means (ppm) at
                             ``cfg.grid_res``. NaN where no retrievals or
                             count < ``cfg.min_retrievals_per_cell``.
        "stderr_grid"      : 2-D float64 numpy array of per-cell stderr (ppm)
                             when ``cfg.inverse_variance_weighting`` is True;
                             otherwise None.
        "count_grid"       : 2-D int64 numpy array of per-cell retrieval counts.
        "stderr_tif_path"  : Path to xco2_composite_stderr.tif when inv-var
                             weighting is enabled; otherwise None.

    ``None`` is returned when no granules are found or all downloads fail.

    Both the inverse-variance and arithmetic-mean paths share the same
    binning kernel (:func:`methodology.grid_inverse_variance`). Passing an
    array of ones for uncertainties reduces the inv-var formula to an
    arithmetic mean (see methodology Property 4), so both modes produce
    identical ``mean_grid`` values when the true uncertainties are constant.
    """
    def _p(msg): prog(25, msg) if prog else log.info(msg)

    try:
        import requests as _requests
    except ImportError:
        _p("❌  'requests' not installed.  Run:  pip install requests")
        return None

    import tempfile

    grid_res = cfg.grid_res
    cmr_spatial_buffer = cfg.cmr_spatial_buffer
    oco2_version = cfg.oco2_version
    oco3_version = cfg.oco3_version
    gosat_version = cfg.gosat_version

    _ensure_earthdata_netrc(earthdata_user, earthdata_pass)

    # Expand AOI for CMR search to account for narrow OCO swath geometry
    buffered_bbox = _buffer_bbox(bbox, cmr_spatial_buffer)

    datasets = []
    if use_oco2:
        datasets.append((OCO2_SHORT_NAME, oco2_version or "", "OCO-2"))
    if use_oco3:
        datasets.append((OCO3_SHORT_NAME, oco3_version or "", "OCO-3"))
    if use_gosat:
        # GOSAT ACOS L2 Lite — same GES DISC auth as OCO; quality flag field
        # is "quality_flag" (not "xco2_quality_flag").
        datasets.append((GOSAT_SHORT_NAME, gosat_version or "", "GOSAT"))

    # Collect direct data URLs from CMR (no OPeNDAP rewriting).
    labelled_urls: list = []
    for short_name, version, label in datasets:
        urls = _cmr_direct_data_urls(short_name, version, start, end, buffered_bbox,
                                     max_granules=8)
        _p(f"OCO: {label} — {len(urls)} granule(s) found via CMR "
           f"(short_name={short_name!r} version={version!r}).")
        # If versioned search returns nothing, retry without a version constraint.
        # GOSAT ACOS version strings vary across archive epochs (e.g. '9r', 'v9r',
        # '10r') — a version-free search finds granules regardless of epoch.
        if not urls and version:
            _p(f"OCO: {label} — retrying CMR search without version constraint …")
            urls = _cmr_direct_data_urls(short_name, "", start, end, buffered_bbox,
                                         max_granules=8)
            _p(f"OCO: {label} — {len(urls)} granule(s) found (no version filter).")
        if not urls:
            _p(f"⚠  {label}: no granules found in CMR for this AOI / date range.")
        for u in urls:
            labelled_urls.append((label, u))

    if not labelled_urls:
        _p("⚠  OCO: no granules found for any dataset. Check credentials and date range.")
        return None

    # Build one authenticated session scoped to data.gesdisc.earthdata.nasa.gov.
    _p(f"OCO: authenticating with NASA EarthData as '{earthdata_user}' …")
    session = None
    try:
        from pydap.cas.urs import setup_session
        session = setup_session(
            earthdata_user, earthdata_pass,
            check_url=labelled_urls[0][1],   # real data URL → cookies scoped correctly
        )
    except Exception as e:
        _p(f"⚠  OCO: pydap setup_session raised {e} — using basic-auth session.")

    if session is None:
        # setup_session can return None in some pydap versions without raising.
        session = _requests.Session()
        session.auth = (earthdata_user, earthdata_pass)
        _p("OCO: using HTTP basic-auth session.")
    else:
        _p("OCO: NASA EarthData session established.")

    all_lats: list = []
    all_lons: list = []
    all_xco2: list = []
    all_unc:  list = []

    for label, url in labelled_urls:
        tmp_path = None
        try:
            fname = url.rsplit("/", 1)[-1]
            _p(f"OCO: downloading {fname} …")
            r = session.get(url, stream=True, timeout=300)
            r.raise_for_status()

            with tempfile.NamedTemporaryFile(suffix=".nc4", delete=False) as f:
                for chunk in r.iter_content(chunk_size=1 << 20):  # 1 MB chunks
                    f.write(chunk)
                tmp_path = f.name

            xco2_arr, lat_arr, lon_arr, qf_arr, unc_arr = _read_nc4_oco_vars(tmp_path)

            # Spatial clause uses the ORIGINAL (un-buffered) bbox so that only
            # retrievals inside the true AOI are kept, regardless of how much
            # the CMR search bbox was expanded.
            in_bbox = _filter_lat_lon_in_bbox(lat_arr, lon_arr, bbox)
            mask = (
                in_bbox &
                (qf_arr == 0) &
                (xco2_arr > 100) & (xco2_arr < 600)
            )
            n_good = int(mask.sum())
            all_lats.extend(lat_arr[mask].tolist())
            all_lons.extend(lon_arr[mask].tolist())
            all_xco2.extend(xco2_arr[mask].tolist())
            all_unc.extend(unc_arr[mask].tolist())
            _p(f"OCO: {label} — {n_good} good retrieval(s) in AOI from {fname}.")

        except Exception as e:
            _p(f"❌  OCO: failed for {url.rsplit('/', 1)[-1]}: {e}")
        finally:
            if tmp_path and os.path.exists(tmp_path):
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass

    if not all_lats:
        log.warning("No OCO-2/3 retrievals found for the specified AOI and date range.")
        return None

    lats_np = np.array(all_lats, dtype=np.float64)
    lons_np = np.array(all_lons, dtype=np.float64)
    xco2_np = np.array(all_xco2, dtype=np.float64)
    unc_np  = np.array(all_unc,  dtype=np.float64)

    # If inverse-variance weighting is disabled, substitute constant σ=1.
    # By methodology Property 4, grid_inverse_variance with constant σ
    # produces the arithmetic mean — same values _grid_oco_to_tif would
    # emit, but we also get count_grid "for free" for min-retrievals masking.
    if cfg.inverse_variance_weighting:
        # Replace non-finite or zero uncertainties with the median σ to avoid
        # a few bad rows monopolizing the weighted mean. This preserves the
        # inv-var property while guarding against sentinel values.
        finite = np.isfinite(unc_np) & (unc_np > 0.0)
        if finite.any():
            median_sigma = float(np.median(unc_np[finite]))
        else:
            median_sigma = 1.0
        unc_effective = np.where(finite, unc_np, median_sigma)
    else:
        unc_effective = np.ones_like(xco2_np, dtype=np.float64)

    mean_grid, stderr_grid, count_grid = methodology.grid_inverse_variance(
        lats_np, lons_np, xco2_np, unc_effective,
        bbox, grid_res,
        min_retrievals=cfg.min_retrievals_per_cell,
    )

    tif_path = _grid_array_to_tif(
        mean_grid, bbox, grid_res,
        out_dir / "xco2_composite.tif", "XCO2_ppm",
    )
    if tif_path is not None:
        _p(f"OCO: XCO₂ composite saved → {tif_path}  "
           f"({count_grid.sum()} retrievals across {int((count_grid > 0).sum())} cells).")

    stderr_tif_path: Optional[Path] = None
    if cfg.inverse_variance_weighting:
        # Mask stderr on the same cells that were masked in mean_grid.
        stderr_to_write = np.where(np.isnan(mean_grid), np.nan, stderr_grid)
        stderr_tif_path = _grid_array_to_tif(
            stderr_to_write, bbox, grid_res,
            out_dir / "xco2_composite_stderr.tif", "XCO2_stderr_ppm",
        )
        if stderr_tif_path is not None:
            _p(f"OCO: XCO₂ stderr raster saved → {stderr_tif_path}.")

    # ── Multi-scale fine-grid composite (Req 14) ─────────────────────────
    # Re-bin the SAME retrievals at cfg.fine_grid_res so OCO's native ~1.3 km
    # resolution isn't wasted by the 0.1° coarse grid. The fine grid is NOT
    # inserted into the composites dict and is NOT cropland-masked here —
    # it's a standalone raster output. Callers still get composites-aware
    # behavior from the coarse grid above.
    fine_tif: Optional[Path] = None
    if cfg.multiscale_fine_grid:
        try:
            fine_mean, _fine_stderr, _fine_count = methodology.grid_inverse_variance(
                lats_np, lons_np, xco2_np, unc_effective,
                bbox, cfg.fine_grid_res,
                min_retrievals=cfg.min_retrievals_per_cell,
            )
            fine_tif = _grid_array_to_tif(
                fine_mean, bbox, cfg.fine_grid_res,
                out_dir / "xco2_composite_fine.tif", "XCO2_ppm",
            )
            if fine_tif is not None:
                _p(f"OCO: fine-grid composite saved → {fine_tif}  "
                   f"(res={cfg.fine_grid_res}°).")
        except Exception as e:
            _p(f"⚠  OCO: fine-grid composite failed: {e}")
            fine_tif = None

    return {
        "tif_path":         tif_path,
        "mean_grid":        mean_grid,
        "stderr_grid":      stderr_grid if cfg.inverse_variance_weighting else None,
        "count_grid":       count_grid,
        "stderr_tif_path":  stderr_tif_path,
        "fine_tif_path":    fine_tif,
    }


def _build_cmr_params(short_name: str,
                      version: Optional[str],
                      start: str, end: str,
                      bbox: list,
                      max_granules: int) -> dict:
    """
    Build the CMR query params dict. The ``version`` key is included only
    when ``version`` is truthy AND non-empty after stripping whitespace.
    ``None``, ``""``, and pure-whitespace strings all omit the key.

    Pure function — no I/O.
    """
    west, south, east, north = bbox
    params = {
        "short_name":   short_name,
        "temporal[]":   f"{start}T00:00:00Z,{end}T23:59:59Z",
        "bounding_box": f"{west},{south},{east},{north}",
        "page_size":    max_granules,
        "sort_key":     "start_date",
    }
    if version and version.strip():
        params["version"] = version
    return params


def _cmr_direct_data_urls(short_name: str, version: str,
                           start: str, end: str, bbox: list,
                           max_granules: int = 8) -> list:
    """
    Query NASA CMR and return direct HTTPS download URLs
    (data.gesdisc.earthdata.nasa.gov/data/...) for each granule.
    No OPeNDAP rewriting — used for direct download mode.
    """
    try:
        import requests
    except ImportError:
        log.error("'requests' not installed.")
        return []

    params = _build_cmr_params(short_name, version, start, end, bbox, max_granules)
    try:
        resp = requests.get(NASA_CMR_SEARCH, params=params, timeout=30)
        resp.raise_for_status()
        entries = resp.json().get("feed", {}).get("entry", [])
    except Exception as e:
        log.warning("CMR search failed for %s v%s: %s", short_name, version, e)
        return []

    urls = []
    for entry in entries:
        for link in entry.get("links", []):
            href = link.get("href", "")
            rel  = link.get("rel", "")
            if "data#" in rel and href.endswith((".nc4", ".nc")):
                urls.append(href)
                break
    return urls


def _read_nc4_oco_vars(path: str):
    """
    Read xco2, latitude, longitude, quality_flag, xco2_uncertainty from an
    OCO-2/3 or GOSAT NC4 file. Handles two quality-flag field names:
      - OCO-2/3  : "xco2_quality_flag"
      - GOSAT ACOS: "quality_flag"
    The uncertainty variable is ``xco2_uncertainty`` on all three products.
    Tries h5py first (bundled with QGIS/OSGeo4W), then netCDF4, then GDAL.
    Returns five flat numpy arrays
    (xco2 f64, lat f64, lon f64, qf i32, unc f64).

    If ``xco2_uncertainty`` is missing from the file, the returned uncertainty
    array is an all-ones vector and a warning is logged. Substituting a
    constant collapses the inverse-variance formula to an arithmetic mean
    (see methodology Property 4), so downstream gridding still produces a
    sensible result without crashing.
    """

    def _pick_qf(keys):
        for name in ("xco2_quality_flag", "quality_flag"):
            if name in keys:
                return name
        raise KeyError(f"No quality-flag field found. Available: {list(keys)}")

    def _maybe_warn_missing_unc(path_: str):
        log.warning(
            "xco2_uncertainty not found in %s — using constant σ=1.0 "
            "(arithmetic mean). Per-cell stderr will NOT reflect true retrieval "
            "uncertainty.", path_,
        )

    try:
        import h5py
        with h5py.File(path, "r") as f:
            qf_key = _pick_qf(f.keys())
            xco2 = np.array(f["xco2"]).flatten().astype(np.float64)
            lat  = np.array(f["latitude"]).flatten().astype(np.float64)
            lon  = np.array(f["longitude"]).flatten().astype(np.float64)
            qf   = np.array(f[qf_key]).flatten().astype(np.int32)
            if "xco2_uncertainty" in f:
                unc = np.array(f["xco2_uncertainty"]).flatten().astype(np.float64)
            else:
                _maybe_warn_missing_unc(path)
                unc = np.ones_like(xco2, dtype=np.float64)
        return xco2, lat, lon, qf, unc
    except ImportError:
        pass

    try:
        import netCDF4 as nc
        with nc.Dataset(path) as ds:
            qf_key = _pick_qf(ds.variables.keys())
            xco2 = np.ma.filled(ds["xco2"][:], np.nan).flatten().astype(np.float64)
            lat  = np.ma.filled(ds["latitude"][:], np.nan).flatten().astype(np.float64)
            lon  = np.ma.filled(ds["longitude"][:], np.nan).flatten().astype(np.float64)
            qf   = np.ma.filled(ds[qf_key][:], 1).flatten().astype(np.int32)
            if "xco2_uncertainty" in ds.variables:
                unc = np.ma.filled(ds["xco2_uncertainty"][:], np.nan).flatten().astype(np.float64)
            else:
                _maybe_warn_missing_unc(path)
                unc = np.ones_like(xco2, dtype=np.float64)
        return xco2, lat, lon, qf, unc
    except ImportError:
        pass

    # GDAL fallback — always bundled with QGIS/OSGeo4W, no pip install needed.
    try:
        from osgeo import gdal as _gdal

        def _gdal_var(var_name):
            ds = _gdal.Open(f'NETCDF:"{path}":{var_name}')
            if ds is None:
                raise RuntimeError(f"GDAL could not open variable '{var_name}' in {path}")
            arr = ds.ReadAsArray().flatten().astype(np.float64)
            ds = None
            return arr

        xco2 = _gdal_var("xco2")
        lat  = _gdal_var("latitude")
        lon  = _gdal_var("longitude")
        qf   = None
        for qf_name in ("xco2_quality_flag", "quality_flag"):
            try:
                qf = _gdal_var(qf_name).astype(np.int32)
                break
            except Exception:
                continue
        if qf is None:
            raise KeyError(f"No quality-flag variable found via GDAL in {path}")
        try:
            unc = _gdal_var("xco2_uncertainty")
        except Exception:
            _maybe_warn_missing_unc(path)
            unc = np.ones_like(xco2, dtype=np.float64)
        return xco2, lat, lon, qf, unc
    except ImportError:
        pass

    raise RuntimeError(
        "Cannot read NC4 file: h5py, netCDF4, and osgeo.gdal are all unavailable. "
        "Run in OSGeo4W Shell:  pip install h5py"
    )


def _ensure_earthdata_netrc(username: str, password: str) -> None:
    """
    Write (or update) the ~/.netrc entry for urs.earthdata.nasa.gov.
    Mirrors the reference repo approach: credentials are stored in .netrc so
    that pydap/requests can satisfy the URS Basic-Auth challenge automatically.
    """
    import netrc as _netrc_mod
    import stat

    netrc_path = Path.home() / ".netrc"
    host = NASA_URS_HOST

    # Read existing entries, if any.
    try:
        existing = _netrc_mod.netrc(str(netrc_path))
        hosts = dict(existing.hosts)
    except Exception:
        hosts = {}

    # Only write if missing or credentials changed.
    current = hosts.get(host)
    if current and current[0] == username and current[2] == password:
        return

    hosts[host] = (username, None, password)

    lines = []
    for h, (login, account, passwd) in hosts.items():
        lines.append(f"machine {h}")
        lines.append(f"  login {login}")
        if account:
            lines.append(f"  account {account}")
        lines.append(f"  password {passwd}")
        lines.append("")

    netrc_path.write_text("\n".join(lines))
    try:
        netrc_path.chmod(stat.S_IRUSR | stat.S_IWUSR)  # 0o600 — required by netrc
    except Exception:
        pass


def _stage_gosat_ch4_nies(start: str, end: str, bbox: list, out_dir: Path,
                           nies_user: str, nies_pass: str,
                           grid_res: float,
                           prog: Optional[ProgressFn] = None) -> Optional[Path]:
    """
    Download GOSAT TANSO-FTS XCH₄ L2 data from the NIES GOSAT Data Archive
    Service (GDAS) via SFTP and write a gridded mean XCH₄ GeoTIFF.

    NIES portal: https://prdct.gosat-2.nies.go.jp
    Register at: https://prdct.gosat-2.nies.go.jp/en/aboutdata/directsftpaccess.html

    Access protocol: SFTP (paramiko)
    Path pattern   : /pub/gosat/SWIRFTS/NIES/L2/YYYYMMDD/
    File pattern   : GOSAT_SWIRFTS_NIES_L2_<date>_*.h5

    Variables (HDF5 path, NIES L2 v02.xx):
      /Data/scanAttribute/latitude
      /Data/scanAttribute/longitude
      /Data/spectralFit/XCH4  (or /CH4/XCH4 depending on product version)
      /Data/spectralFit/XCH4_quality  (0 = good)

    Requires: pip install paramiko h5py
    """
    def _p(msg): prog(30, msg) if prog else log.info(msg)

    try:
        import paramiko
    except ImportError:
        _p("❌  GOSAT XCH₄: 'paramiko' not installed. "
           "Run in OSGeo4W Shell:  pip install paramiko")
        return None

    import numpy as np
    import tempfile
    import os
    from datetime import date, timedelta

    _p(f"GOSAT XCH₄: connecting to NIES SFTP as '{nies_user}' …")

    try:
        transport = paramiko.Transport((NIES_SFTP_HOST, 22))
        transport.connect(username=nies_user, password=nies_pass)
        sftp = paramiko.SFTPClient.from_transport(transport)
        _p("GOSAT XCH₄: NIES SFTP session established.")
    except Exception as e:
        _p(f"❌  GOSAT XCH₄: SFTP connection failed: {e}")
        return None

    # ── Discover the user's actual working directory ─────────────────────
    # NIES SFTP server uses a chroot jail. sftp.listdir("/") shows the chroot
    # root (typically ['pub','dev','etc']) but all those dirs are Permission
    # Denied. The real data lives relative to the user's home inside the jail.
    # getcwd() returns the home path; we probe from there with relative paths.
    try:
        cwd = sftp.getcwd() or "."
        _p(f"GOSAT XCH₄: SFTP working directory (getcwd): {cwd!r}")
    except Exception as e:
        cwd = "."
        _p(f"GOSAT XCH₄: getcwd failed ({e}), using '.'")

    # Probe from home dir so we can log the real directory layout.
    for probe in [cwd, ".", "pub", "data", "gosat", "GOSAT"]:
        try:
            entries = sftp.listdir(probe)
            _p(f"GOSAT XCH₄: probe {probe!r} → {entries[:12]}")
            for entry in entries[:6]:
                child = f"{probe}/{entry}".lstrip("./")
                child = child if child else entry
                try:
                    sub = sftp.listdir(child)
                    _p(f"GOSAT XCH₄:   {child}/ → {sub[:8]}")
                    for sub_entry in sub[:4]:
                        grandchild = f"{child}/{sub_entry}"
                        try:
                            deep = sftp.listdir(grandchild)
                            _p(f"GOSAT XCH₄:     {grandchild}/ → {deep[:6]}")
                        except Exception as e3:
                            _p(f"GOSAT XCH₄:     {grandchild}/ → (error: {e3})")
                except Exception as e2:
                    _p(f"GOSAT XCH₄:   {child}/ → (error: {e2})")
        except Exception as e:
            _p(f"GOSAT XCH₄: probe {probe!r} → (error: {e})")

    # Build the base data path. NIES typically organises data as:
    #   <home>/pub/gosat/SWIRFTS/NIES/L2/YYYY/YYYYMMDD/
    # but if the chroot home IS the pub dir the leading segment differs.
    # We check both and fall back to the relative path that resolves.
    def _resolve_base() -> str:
        candidates = [
            "pub/gosat/SWIRFTS/NIES/L2",
            "gosat/SWIRFTS/NIES/L2",
            "SWIRFTS/NIES/L2",
            "NIES/L2",
            "L2",
        ]
        for c in candidates:
            try:
                sftp.stat(c)
                _p(f"GOSAT XCH₄: using data base path {c!r}")
                return c
            except Exception:
                pass
        _p("GOSAT XCH₄: could not resolve data base path — will attempt default")
        return "pub/gosat/SWIRFTS/NIES/L2"

    data_base = _resolve_base()

    west, south, east, north = bbox
    all_lats: list = []
    all_lons: list = []
    all_ch4:  list = []

    try:
        # Walk day-by-day over the date range; limit to avoid very long runs.
        start_dt = date.fromisoformat(start)
        end_dt   = date.fromisoformat(end)
        days_total = (end_dt - start_dt).days + 1
        step = max(1, days_total // 30)   # sample ≤30 days across the range

        for offset in range(0, days_total, step):
            day = start_dt + timedelta(days=offset)
            day_str  = day.strftime("%Y%m%d")
            year_str = day.strftime("%Y")
            remote_dir = f"{data_base}/{year_str}/{day_str}"

            try:
                files = sftp.listdir(remote_dir)
            except IOError:
                continue   # directory may not exist (no overpass that day)

            h5_files = [f for f in files if f.endswith(".h5")]
            for fname in h5_files[:3]:   # at most 3 files per day
                tmp_path = None
                try:
                    with tempfile.NamedTemporaryFile(suffix=".h5", delete=False) as tf:
                        tmp_path = tf.name
                    sftp.get(f"{remote_dir}/{fname}", tmp_path)

                    import h5py
                    with h5py.File(tmp_path, "r") as f:
                        # Try both known variable path layouts.
                        if "Data/spectralFit/XCH4" in f:
                            ch4_raw = np.array(f["Data/spectralFit/XCH4"]).flatten()
                            lat_raw = np.array(f["Data/scanAttribute/latitude"]).flatten()
                            lon_raw = np.array(f["Data/scanAttribute/longitude"]).flatten()
                            qf_raw  = np.array(f["Data/spectralFit/XCH4_quality"]).flatten()
                        elif "CH4/XCH4" in f:
                            ch4_raw = np.array(f["CH4/XCH4"]).flatten()
                            lat_raw = np.array(f["CH4/latitude"]).flatten()
                            lon_raw = np.array(f["CH4/longitude"]).flatten()
                            qf_raw  = np.array(f["CH4/quality_flag"]).flatten()
                        else:
                            _p(f"⚠  GOSAT XCH₄: unrecognised HDF5 layout in {fname} — skipping.")
                            continue

                    mask = (
                        (qf_raw == 0) &
                        (lat_raw >= south) & (lat_raw <= north) &
                        (lon_raw >= west)  & (lon_raw <= east)  &
                        (ch4_raw > 1200)   & (ch4_raw < 2200)   # ppb physical range
                    )
                    all_lats.extend(lat_raw[mask].tolist())
                    all_lons.extend(lon_raw[mask].tolist())
                    all_ch4.extend(ch4_raw[mask].tolist())
                    _p(f"GOSAT XCH₄: {int(mask.sum())} good retrieval(s) in AOI from {fname}.")

                except Exception as e:
                    _p(f"❌  GOSAT XCH₄: error reading {fname}: {e}")
                finally:
                    if tmp_path and os.path.exists(tmp_path):
                        try:
                            os.unlink(tmp_path)
                        except OSError:
                            pass
    finally:
        sftp.close()
        transport.close()

    if not all_lats:
        log.warning("GOSAT XCH4: no retrievals found for AOI / date range.")
        return None

    return _grid_oco_to_tif(
        np.array(all_lats, dtype=np.float64),
        np.array(all_lons, dtype=np.float64),
        np.array(all_ch4,  dtype=np.float64),
        bbox, grid_res,
        out_dir / "gosat_ch4_composite.tif",
    )


def _grid_array_to_tif(arr: "np.ndarray",
                        bbox: list,
                        grid_res: float,
                        out_path: Path,
                        band_description: str = "data") -> Optional[Path]:
    """Write a 2-D numpy array to a GeoTIFF using GDAL.

    Output conventions match :func:`_grid_oco_to_tif`:
      * EPSG:4326 lat/lon projection
      * LZW + TILED options
      * NaN NoData (float32 band)
      * Geotransform anchored at the bbox NW corner (row 0 at ``north``)

    The grid extent is inferred from ``arr.shape`` (``rows, cols``). If the
    input shape does not match ``ceil((east - west) / grid_res)`` × ``ceil
    ((north - south) / grid_res)`` the caller is responsible for supplying
    a consistent bbox/grid_res pair — this function trusts the caller.

    Never raises: on any GDAL error the exception is logged and ``None`` is
    returned. Output directory is created if needed.
    """
    try:
        from osgeo import gdal, osr
    except ImportError:
        log.error("osgeo.gdal not available — cannot write GeoTIFF %s", out_path)
        return None

    try:
        arr = np.asarray(arr, dtype=np.float32)
        if arr.ndim != 2:
            log.error("_grid_array_to_tif: expected 2-D array, got shape %s", arr.shape)
            return None

        out_path = Path(out_path)
        out_path.parent.mkdir(parents=True, exist_ok=True)

        west, south, east, north = [float(x) for x in bbox]
        ny, nx = arr.shape

        driver = gdal.GetDriverByName("GTiff")
        ds = driver.Create(str(out_path), nx, ny, 1, gdal.GDT_Float32,
                           options=["COMPRESS=LZW", "TILED=YES"])
        ds.SetGeoTransform([west, grid_res, 0, north, 0, -grid_res])
        srs = osr.SpatialReference()
        srs.ImportFromEPSG(4326)
        ds.SetProjection(srs.ExportToWkt())
        band = ds.GetRasterBand(1)
        band.WriteArray(arr)
        band.SetNoDataValue(float("nan"))
        band.SetDescription(band_description)
        ds.FlushCache()
        ds = None

        log.info("GeoTIFF saved → %s  (%d×%d grid, band=%s)",
                 out_path, nx, ny, band_description)
        return out_path

    except Exception as e:
        log.warning("_grid_array_to_tif(%s) failed: %s", out_path, e)
        return None


def _grid_oco_to_tif(lats, lons, values, bbox: list,
                     grid_res: float, out_path: Path) -> Optional[Path]:
    """
    Bin sparse OCO point retrievals onto a regular grid and write a GeoTIFF.

    Kept for backward compatibility: callers using the legacy arithmetic-mean
    gridding path still get the same output. Internally this delegates the
    raster I/O to :func:`_grid_array_to_tif` so the GDAL writing logic lives
    in one place.
    """
    west, south, east, north = bbox
    nx = max(1, int(round((east - west)  / grid_res)))
    ny = max(1, int(round((north - south) / grid_res)))

    grid_sum = np.zeros((ny, nx), dtype=np.float64)
    grid_cnt = np.zeros((ny, nx), dtype=np.int32)

    for lat, lon, val in zip(lats, lons, values):
        xi = int((lon - west)  / grid_res)
        yi = int((north - lat) / grid_res)
        if 0 <= xi < nx and 0 <= yi < ny:
            grid_sum[yi, xi] += val
            grid_cnt[yi, xi] += 1

    with np.errstate(invalid="ignore", divide="ignore"):
        grid_mean = np.where(grid_cnt > 0,
                             grid_sum / grid_cnt,
                             np.nan).astype(np.float32)

    result = _grid_array_to_tif(grid_mean, bbox, grid_res, Path(out_path), "XCO2_ppm")
    if result is not None:
        log.info("XCO₂ composite GeoTIFF saved → %s  (%d×%d grid, %d retrievals)",
                 out_path, nx, ny, int(grid_cnt.sum()))
    return result


# ═══════════════════════════════════════════════════════════════════════════
# CMR version auto-discovery (Task 10.1 / Req 16)
# ═══════════════════════════════════════════════════════════════════════════

def _natural_sort_key(s: str):
    """Sort key that treats embedded numbers naturally (v9r < v10r < v11r).

    Splits the input on runs of digits so numeric tokens sort by value rather
    than lexicographically. Used by :func:`discover_cmr_versions` to order
    CMR ``version_id`` strings like ``'9r' < '10r' < '11r' < '11.2r'``.
    """
    import re
    return [int(t) if t.isdigit() else t for t in re.split(r"(\d+)", str(s))]


def discover_cmr_versions(short_name: str, timeout: float = 15.0) -> list:
    """Query CMR and return versions sorted naturally, newest first.

    Hits ``cmr.earthdata.nasa.gov/search/collections.json`` with the given
    ``short_name`` and deduplicates ``version_id`` across the response
    entries. Returns ``[]`` on any network / JSON / parsing error — this
    helper is used by the Setup tab's "Refresh versions from CMR" button
    and must never raise (Req 16.4).

    Parameters
    ----------
    short_name : str
        CMR ``short_name`` (e.g. ``"OCO2_L2_Lite_FP"``).
    timeout : float, default 15.0
        HTTP timeout in seconds.
    """
    try:
        import requests
    except ImportError:
        log.warning("discover_cmr_versions: 'requests' not installed.")
        return []
    try:
        resp = requests.get(
            NASA_CMR_COLLECTIONS,
            params={"short_name": short_name, "page_size": 100},
            timeout=timeout,
        )
        resp.raise_for_status()
        entries = resp.json().get("feed", {}).get("entry", [])
        versions = {e.get("version_id") for e in entries if e.get("version_id")}
        return sorted(versions, key=_natural_sort_key, reverse=True)
    except Exception as e:
        log.warning("discover_cmr_versions(%s) failed: %s", short_name, e)
        return []


# ═══════════════════════════════════════════════════════════════════════════
# CAAQMS bias correction (Task 7.1 / Req 12)
# ═══════════════════════════════════════════════════════════════════════════

def _apply_caaqms_bias_correction(
    cfg: "PipelineConfig",
    caaqms_csv: str,
    composites: dict,
    bbox: list,
    grid_res: float,
    win_dir: Path,
    win_prog,
) -> dict:
    """Interpolate station-level mean bias via IDW and subtract it from composites.

    For each station in the CAAQMS CSV we:
      1. Parse lat/lon and the pollutant column for CH4 / CO2 (alternate
         column names are tried in order).
      2. Compute the station's mean value across all its CSV rows.
      3. Look up the co-located cell in ``composites["ch4_ppb"]`` /
         ``composites["xco2_ppm"]`` and form ``bias = station_mean - sat_val``.
      4. If the co-located satellite cell is NaN, the station is dropped for
         that pollutant.

    The per-pollutant bias points are interpolated to the AOI grid via
    :func:`methodology.idw_interpolate` with ``cfg.idw_power`` and subtracted
    from the corresponding composite. The corrected rasters are written to
    ``win_dir`` and — crucially — ``composites["ch4_ppb"]`` and
    ``composites["xco2_ppm"]`` are updated IN PLACE so downstream enhancement
    / flux / hotspot computations see the calibrated values
    (design.md §2, Req 12.3).

    Parameters
    ----------
    cfg : PipelineConfig
        Reads ``cfg.idw_power``.
    caaqms_csv : str
        Path to the station CSV (UTF-8-sig, DictReader-compatible).
    composites : dict
        In/out. ``ch4_ppb`` / ``xco2_ppm`` 2-D numpy arrays are replaced
        with bias-corrected versions when this helper finds enough stations.
    bbox : [west, south, east, north]
    grid_res : float
        Cell size (degrees).
    win_dir : Path
        Per-window output directory — this is where the corrected rasters
        are written (``ch4_bias_corrected.tif`` / ``xco2_bias_corrected.tif``).
    win_prog : callable(pct:int, msg:str)
        Progress callback.

    Returns
    -------
    dict
        ``{"ch4_bias_corrected_tif_path": Path|None,
           "xco2_bias_corrected_tif_path": Path|None}``.
        An empty dict on any hard failure. Never raises.
    """
    out: Dict[str, Optional[Path]] = {}

    try:
        import csv as _csv
    except Exception as e:  # pragma: no cover — stdlib
        log.warning("_apply_caaqms_bias_correction: csv import failed: %s", e)
        return {}

    # ── 1. Parse station rows, grouping by a (lat, lon) station key ──────
    # CAAQMS CSVs repeat station metadata on every row; we aggregate per
    # station to one mean value per pollutant.
    try:
        with open(caaqms_csv, newline="", encoding="utf-8-sig") as fh:
            reader = _csv.DictReader(fh)
            all_rows = list(reader)
    except Exception as e:
        win_prog(48, f"⚠  CAAQMS bias: could not read {caaqms_csv}: {e}")
        return {}

    if not all_rows:
        win_prog(48, "⚠  CAAQMS bias: CSV has no rows.")
        return {}

    # Identify coordinate columns (case-tolerant).
    field_names = [f for f in (all_rows[0].keys() or []) if f]
    lat_key = next((k for k in field_names
                    if k.lower() in ("lat", "latitude", "station_lat")), None)
    lon_key = next((k for k in field_names
                    if k.lower() in ("lon", "lng", "longitude", "station_lon")), None)
    if lat_key is None or lon_key is None:
        win_prog(48, "⚠  CAAQMS bias: no lat/lon columns found — skipping.")
        return {}

    def _pick_col(row: dict, candidates: list) -> Optional[str]:
        """Return the first key in ``candidates`` present (case-sensitive)
        in ``row``, matching the order given."""
        for name in candidates:
            if name in row:
                return name
        return None

    ch4_candidates = ["CH4", "ch4", "Methane"]
    co2_candidates = ["CO2", "co2", "Carbon Dioxide"]

    # Aggregate per (lat, lon, pollutant).
    from collections import defaultdict
    ch4_station_vals: Dict[Tuple[float, float], list] = defaultdict(list)
    co2_station_vals: Dict[Tuple[float, float], list] = defaultdict(list)

    for row in all_rows:
        try:
            lat = float(row.get(lat_key, "") or "nan")
            lon = float(row.get(lon_key, "") or "nan")
        except (TypeError, ValueError):
            continue
        if not (math.isfinite(lat) and math.isfinite(lon)):
            continue
        key = (lat, lon)

        ch4_col = _pick_col(row, ch4_candidates)
        if ch4_col is not None:
            raw = row.get(ch4_col, "")
            if raw not in ("", "NA", "NaN", "null", "NULL", "--"):
                try:
                    ch4_station_vals[key].append(float(raw))
                except (TypeError, ValueError):
                    pass

        co2_col = _pick_col(row, co2_candidates)
        if co2_col is not None:
            raw = row.get(co2_col, "")
            if raw not in ("", "NA", "NaN", "null", "NULL", "--"):
                try:
                    co2_station_vals[key].append(float(raw))
                except (TypeError, ValueError):
                    pass

    # ── 2-5. Per-pollutant bias field + subtract from composite ─────────
    west, south, east, north = [float(x) for x in bbox]

    def _process_pollutant(
        station_vals: Dict[Tuple[float, float], list],
        composite_key: str,
        out_tif_name: str,
        band_desc: str,
        pol_label: str,
    ) -> Optional[Path]:
        sat_grid = composites.get(composite_key)
        if sat_grid is None:
            return None
        if not isinstance(sat_grid, np.ndarray) or sat_grid.ndim != 2:
            return None
        if not station_vals:
            win_prog(48, f"⚠  CAAQMS bias: no {pol_label} station values — skipping.")
            return None

        rows_g, cols_g = sat_grid.shape
        # Build the station point+bias list by co-locating with composite cells.
        pts_xy: list = []
        biases: list = []
        dropped_out_of_aoi = 0
        dropped_nan_cell = 0
        for (lat, lon), vals in station_vals.items():
            if not vals:
                continue
            if not (south <= lat <= north and west <= lon <= east):
                dropped_out_of_aoi += 1
                continue
            # Row 0 is at the north edge (matches _grid_array_to_tif).
            row_idx = int((north - lat) / grid_res)
            col_idx = int((lon - west) / grid_res)
            row_idx = min(max(row_idx, 0), rows_g - 1)
            col_idx = min(max(col_idx, 0), cols_g - 1)
            sat_val = sat_grid[row_idx, col_idx]
            if not np.isfinite(sat_val):
                dropped_nan_cell += 1
                continue
            station_mean = float(np.mean(vals))
            bias = station_mean - float(sat_val)
            pts_xy.append([lon, lat])
            biases.append(bias)

        if not pts_xy:
            win_prog(
                48,
                f"⚠  CAAQMS bias: no co-located {pol_label} stations "
                f"(dropped out-of-AOI={dropped_out_of_aoi}, "
                f"NaN-cell={dropped_nan_cell}) — skipping.",
            )
            return None

        # Interpolate bias field to the AOI grid using cell centers.
        grid_x = west + (np.arange(cols_g) + 0.5) * grid_res
        grid_y = north - (np.arange(rows_g) + 0.5) * grid_res  # row 0 at north

        try:
            bias_field = methodology.idw_interpolate(
                np.asarray(pts_xy, dtype=np.float64),
                np.asarray(biases, dtype=np.float64),
                grid_x, grid_y,
                power=float(cfg.idw_power),
            )
        except Exception as e:
            win_prog(48, f"⚠  CAAQMS bias: {pol_label} IDW failed: {e}")
            return None

        # Subtract bias from satellite composite; preserve NaN from sat_grid.
        corrected = sat_grid.astype(np.float64) - bias_field
        corrected = np.where(np.isnan(sat_grid), np.nan, corrected)

        # Update composites IN PLACE so enhancement / flux / hotspots use
        # the bias-corrected arrays (Req 12.3, design.md §2).
        composites[composite_key] = corrected.astype(sat_grid.dtype, copy=False)

        out_path = _grid_array_to_tif(
            corrected, bbox, grid_res,
            win_dir / out_tif_name, band_desc,
        )
        if out_path is not None:
            win_prog(
                48,
                f"CAAQMS bias: {pol_label} corrected "
                f"({len(pts_xy)} stations) → {out_path.name}",
            )
        return out_path

    try:
        ch4_path = _process_pollutant(
            ch4_station_vals, "ch4_ppb",
            "ch4_bias_corrected.tif", "CH4_ppb_bias_corrected", "CH4",
        )
        out["ch4_bias_corrected_tif_path"] = ch4_path
    except Exception as e:
        win_prog(48, f"⚠  CAAQMS bias: CH4 branch failed: {e}")

    try:
        xco2_path = _process_pollutant(
            co2_station_vals, "xco2_ppm",
            "xco2_bias_corrected.tif", "XCO2_ppm_bias_corrected", "XCO2",
        )
        out["xco2_bias_corrected_tif_path"] = xco2_path
    except Exception as e:
        win_prog(48, f"⚠  CAAQMS bias: XCO2 branch failed: {e}")

    return out


# ═══════════════════════════════════════════════════════════════════════════
# SOC point rasterization (Task 7.6 / Req 13)
# ═══════════════════════════════════════════════════════════════════════════

def _rasterize_soc_points(
    soc_records: list,
    bbox: list,
    grid_res: float,
    power: float = 2.0,
) -> Optional[np.ndarray]:
    """Rasterize SOC field points onto the AOI grid via IDW.

    Parameters
    ----------
    soc_records : list of dict
        Each record must provide ``lat``, ``lon``, and ``soc_pct`` (or the
        legacy ``soc`` key). Records missing any of these or with non-finite
        values are dropped.
    bbox : [west, south, east, north]
    grid_res : float
        Cell size in degrees.
    power : float, default 2.0
        IDW power exponent (``cfg.idw_power``).

    Returns
    -------
    np.ndarray, shape (rows, cols), dtype float32
        SOC percent interpolated to the AOI grid. ``None`` if ``soc_records``
        is empty or every record is invalid.
    """
    if not soc_records:
        return None

    west, south, east, north = [float(x) for x in bbox]
    if grid_res <= 0:
        log.warning("_rasterize_soc_points: grid_res must be > 0; got %s", grid_res)
        return None

    pts_xy: list = []
    vals: list = []
    for rec in soc_records:
        if not isinstance(rec, dict):
            continue
        try:
            lat = float(rec.get("lat"))
            lon = float(rec.get("lon"))
        except (TypeError, ValueError):
            continue
        # Accept both the new 'soc_pct' and the legacy 'soc' key name.
        raw_soc = rec.get("soc_pct", rec.get("soc"))
        try:
            soc = float(raw_soc)
        except (TypeError, ValueError):
            continue
        if not (math.isfinite(lat) and math.isfinite(lon) and math.isfinite(soc)):
            continue
        pts_xy.append([lon, lat])
        vals.append(soc)

    if not pts_xy:
        return None

    cols = max(1, int(math.ceil((east - west) / grid_res)))
    rows = max(1, int(math.ceil((north - south) / grid_res)))
    grid_x = west + (np.arange(cols) + 0.5) * grid_res
    grid_y = north - (np.arange(rows) + 0.5) * grid_res  # row 0 at north

    try:
        out = methodology.idw_interpolate(
            np.asarray(pts_xy, dtype=np.float64),
            np.asarray(vals, dtype=np.float64),
            grid_x, grid_y,
            power=float(power),
        )
    except Exception as e:
        log.warning("_rasterize_soc_points: IDW failed: %s", e)
        return None

    return out.astype(np.float32)


def _detect_hotspots_v2(cfg: "PipelineConfig",
                         window: dict,
                         composites: dict,
                         out_path: Path,
                         prog: Optional[ProgressFn] = None) -> Optional[Path]:
    """Build a richly-annotated hotspot GeoDataFrame from per-cell composites
    and write it to a GeoPackage.

    ``composites`` may contain any subset of the keys defined in design.md §4:

      * ``ch4_ppb``, ``xco2_ppm``                             (either may be None)
      * ``ch4_stderr``, ``xco2_stderr``                       (if inv-var enabled)
      * ``ch4_count``,  ``xco2_count``                        (always when known)
      * ``ch4_enhancement``, ``xco2_enhancement``             (if enhancement mode)
      * ``ch4_background``,  ``xco2_background``              (if enhancement mode)
      * ``ch4_flux_kg_ha_day``, ``xco2_flux_kg_ha_day``       (if estimate_flux)
      * ``temp_c``, ``wind_speed_ms``, ``wind_dir_deg``,
        ``precip_mm``, ``soil_moist``                         (if use_era5)
      * ``upwind_source_lat``, ``upwind_source_lon``          (if use_era5)
      * ``no2_column``, ``source_type``                       (if use_no2_cotracer)
      * ``fire_count_viirs``, ``fire_count_modis``,
        ``fire_count_total``                                  (if use_firms)
      * ``cattle_density``, ``buffalo_density``,
        ``goat_density``, ``sheep_density``,
        ``enteric_ch4_ipcc_kg_ha_day``                        (if use_livestock)
      * ``cropland_fraction``                                 (if cropland_mask)
      * ``soc_pct``                                           (if SOC rasterised)
      * ``priority_score``, ``priority_rank``                 (if compute_priority)
      * ``hotspot_confidence_pct``                            (enh + inv-var)

    Parameters
    ----------
    cfg : PipelineConfig
        Used for ``aoi_bounds``, ``grid_res`` and threshold / flag reads.
    window : dict
        One entry from :func:`methodology.split_composite_windows`. Only the
        ``name`` key is used (written into the output attributes for cross-
        window joins).
    composites : dict
        Dict of 2-D numpy arrays keyed as above. All arrays must share the
        same ``(rows, cols)`` shape — the first non-None 2-D ndarray in the
        dict defines the expected shape; other entries with mismatched
        shapes are dropped with a warning.
    out_path : Path
        Target GeoPackage path (``*.gpkg``). Parent directories are created.
    prog : ProgressFn, optional
        Progress callback.

    Returns
    -------
    Path
        ``out_path`` on success.
    None
        On any failure (geopandas missing, no valid grid found, GDAL error).
        The function never raises — errors are logged and None is returned.

    Notes
    -----
    * Hotspot flag logic:
        - When ``cfg.enhancement_mode`` is True: a cell is a hotspot if
          ``enhancement >= threshold`` AND, if ``cropland_mask`` is also on,
          ``cropland_fraction > 0``.
        - Otherwise: legacy 90th-percentile classification across the whole
          grid (excluding NaN).
    * ``insufficient_data`` is True when both CH₄ and XCO₂ retrieval counts
      fall below ``cfg.min_retrievals_per_cell``, OR when the cell lies
      outside cropland while ``cropland_mask`` is enabled.
    * ``hotspot_confidence_pct`` is currently only populated for XCO₂.
      TROPOMI L3 does not provide per-pixel retrieval uncertainty, so a
      CH₄ stderr grid would be synthetic; we leave it unset until strict-L2
      CH₄ gridding adopts inverse-variance weighting too.
    * When geopandas is unavailable, the function falls back to GeoJSON
      output (same behavior as :func:`_export_fc_to_gpkg`).
    """
    def _p(msg): prog(70, msg) if prog else log.info(msg)

    try:
        # ── Determine grid shape from any 2-D array in composites ────────
        grid_shape: Optional[tuple] = None
        for v in composites.values():
            if isinstance(v, np.ndarray) and v.ndim == 2:
                grid_shape = v.shape
                break
        if grid_shape is None:
            _p("⚠  _detect_hotspots_v2: no 2-D numpy arrays in composites — "
               "writing empty GeoPackage.")
            return _write_empty_hotspot_gpkg(out_path, _p)

        rows, cols = grid_shape
        west, south, east, north = [float(x) for x in cfg.aoi_bounds]
        # Cell centroids. Row 0 is at the north edge.
        lon_centers = west + (np.arange(cols) + 0.5) * cfg.grid_res
        lat_centers = north - (np.arange(rows) + 0.5) * cfg.grid_res

        # ── Req 15.5: derive hotspot_confidence_pct if both modes are on ──
        if (cfg.enhancement_mode and cfg.inverse_variance_weighting
                and composites.get("xco2_enhancement") is not None
                and composites.get("xco2_stderr") is not None
                and "hotspot_confidence_pct" not in composites):
            try:
                composites["hotspot_confidence_pct"] = methodology.hotspot_confidence_pct(
                    composites["xco2_enhancement"],
                    composites["xco2_stderr"],
                    cfg.xco2_threshold_ppm,
                )
            except Exception as e:
                _p(f"⚠  hotspot_confidence_pct calc failed: {e}")

        # ── Per-cell hotspot flags ───────────────────────────────────────
        ch4_arr = composites.get("ch4_ppb")
        xco2_arr = composites.get("xco2_ppm")

        is_hotspot_ch4 = _compute_hotspot_flag(
            cfg, composites.get("ch4_enhancement"), ch4_arr,
            cfg.ch4_threshold_ppb, composites.get("cropland_fraction"),
        )
        is_hotspot_xco2 = _compute_hotspot_flag(
            cfg, composites.get("xco2_enhancement"), xco2_arr,
            cfg.xco2_threshold_ppm, composites.get("cropland_fraction"),
        )

        # ── insufficient_data flag ───────────────────────────────────────
        ch4_count = composites.get("ch4_count")
        xco2_count = composites.get("xco2_count")
        cropland = composites.get("cropland_fraction")
        insufficient = _compute_insufficient_data_flag(
            cfg, grid_shape, ch4_count, xco2_count, cropland,
        )

        # ── Build flat records list ──────────────────────────────────────
        # Only iterate over cells that have at least some data (or a hotspot
        # flag). We emit every cell so the output grid is complete and
        # externally joinable; a sparse output would be surprising.
        scalar_keys_to_include = _collect_scalar_columns(composites, grid_shape, _p)
        _p(f"Hotspot GeoPackage: building records for {rows}×{cols} = "
           f"{rows * cols} cells, {len(scalar_keys_to_include)} attr column(s).")

        try:
            import geopandas as gpd
            from shapely.geometry import Point
        except Exception as e:
            _p(f"⚠  geopandas unavailable ({e}) — falling back to GeoJSON.")
            return _write_hotspot_geojson(
                out_path, cfg, window, grid_shape,
                lat_centers, lon_centers,
                composites, scalar_keys_to_include,
                is_hotspot_ch4, is_hotspot_xco2, insufficient,
            )

        records: List[dict] = []
        for r in range(rows):
            for c in range(cols):
                rec: Dict[str, Any] = {
                    "window_name":    window.get("name", ""),
                    "row":            int(r),
                    "col":            int(c),
                    "lat":            float(lat_centers[r]),
                    "lon":            float(lon_centers[c]),
                    "is_hotspot_ch4": bool(is_hotspot_ch4[r, c]) if is_hotspot_ch4 is not None else False,
                    "is_hotspot_xco2": bool(is_hotspot_xco2[r, c]) if is_hotspot_xco2 is not None else False,
                    "insufficient_data": bool(insufficient[r, c]),
                    "geometry":       Point(float(lon_centers[c]), float(lat_centers[r])),
                }
                for k in scalar_keys_to_include:
                    arr = composites[k]
                    val = arr[r, c]
                    # Convert numpy scalars to plain python types so
                    # geopandas writes them as OGR fields rather than blobs.
                    if isinstance(val, np.floating):
                        rec[k] = float(val)
                    elif isinstance(val, np.integer):
                        rec[k] = int(val)
                    else:
                        rec[k] = val.item() if hasattr(val, "item") else val
                records.append(rec)

        out_path = Path(out_path)
        out_path.parent.mkdir(parents=True, exist_ok=True)

        gdf = gpd.GeoDataFrame(records, geometry="geometry", crs="EPSG:4326")
        gdf.to_file(str(out_path), driver="GPKG")
        _p(f"✓  Hotspot GeoPackage saved → {out_path.name}  "
           f"({len(gdf)} cells, {int(gdf['is_hotspot_ch4'].sum())} CH₄ hotspots, "
           f"{int(gdf['is_hotspot_xco2'].sum())} XCO₂ hotspots).")
        return out_path

    except Exception as e:
        _p(f"❌  _detect_hotspots_v2 failed: {e}")
        log.exception("_detect_hotspots_v2 exception")
        return None


def _collect_scalar_columns(composites: dict,
                             grid_shape: tuple,
                             log_fn) -> List[str]:
    """Filter composites dict to the 2-D scalar arrays with matching shape."""
    keep: List[str] = []
    for k, v in composites.items():
        if k in ("fires_gdf",):   # non-array auxiliary payloads
            continue
        if not isinstance(v, np.ndarray):
            continue
        if v.ndim != 2:
            log_fn(f"⚠  composite {k!r} is not 2-D (shape={v.shape}) — dropped.")
            continue
        if v.shape != grid_shape:
            log_fn(f"⚠  composite {k!r} shape {v.shape} mismatches grid "
                    f"{grid_shape} — dropped.")
            continue
        keep.append(k)
    return keep


def _compute_hotspot_flag(cfg: "PipelineConfig",
                           enhancement: Optional["np.ndarray"],
                           concentration: Optional["np.ndarray"],
                           threshold: float,
                           cropland: Optional["np.ndarray"]) -> Optional["np.ndarray"]:
    """Return a bool grid flagging hotspot cells per Req 1.5 / 1.6 / 2.4.

    * Enhancement mode: ``enhancement >= threshold`` AND (``cropland > 0`` if
      cropland_mask is on). Cells where ``enhancement`` is NaN → False.
    * Legacy percentile mode: ``concentration >= nanpercentile(90)``.

    Returns ``None`` if both ``enhancement`` and ``concentration`` are None
    (so the caller writes an all-False flag without a source array).
    """
    if cfg.enhancement_mode:
        if enhancement is None:
            return None
        with np.errstate(invalid="ignore"):
            flag = np.where(np.isnan(enhancement), False, enhancement >= threshold)
        if cfg.cropland_mask and cropland is not None and cropland.shape == enhancement.shape:
            flag = flag & (np.nan_to_num(cropland, nan=0.0) > 0)
        return flag.astype(bool)

    # Legacy percentile path.
    if concentration is None:
        return None
    finite = np.isfinite(concentration)
    if not np.any(finite):
        return np.zeros_like(concentration, dtype=bool)
    pct_thresh = float(np.nanpercentile(concentration, HOTSPOT_PERCENTILE))
    with np.errstate(invalid="ignore"):
        flag = np.where(np.isnan(concentration), False, concentration >= pct_thresh)
    return flag.astype(bool)


def _compute_insufficient_data_flag(cfg: "PipelineConfig",
                                     grid_shape: tuple,
                                     ch4_count: Optional["np.ndarray"],
                                     xco2_count: Optional["np.ndarray"],
                                     cropland: Optional["np.ndarray"]) -> "np.ndarray":
    """Return a bool grid of cells flagged as 'insufficient data' per Req 11.2."""
    thr = int(cfg.min_retrievals_per_cell)
    below_ch4 = None
    below_xco2 = None
    if ch4_count is not None and ch4_count.shape == grid_shape:
        below_ch4 = ch4_count < thr
    if xco2_count is not None and xco2_count.shape == grid_shape:
        below_xco2 = xco2_count < thr

    if below_ch4 is not None and below_xco2 is not None:
        below = below_ch4 & below_xco2
    elif below_ch4 is not None:
        below = below_ch4
    elif below_xco2 is not None:
        below = below_xco2
    else:
        below = np.zeros(grid_shape, dtype=bool)

    if cfg.cropland_mask and cropland is not None and cropland.shape == grid_shape:
        below = below | (np.nan_to_num(cropland, nan=0.0) <= 0)

    return below.astype(bool)


def _write_empty_hotspot_gpkg(out_path: Path, log_fn) -> Optional[Path]:
    """Write a zero-feature GeoPackage (or JSON fallback) so downstream code
    sees a file at the expected path even when no composites were produced."""
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        import geopandas as gpd
        gdf = gpd.GeoDataFrame(
            {"window_name": [], "is_hotspot_ch4": [], "is_hotspot_xco2": [],
             "insufficient_data": []},
            geometry=[],
            crs="EPSG:4326",
        )
        gdf.to_file(str(out_path), driver="GPKG")
        return out_path
    except Exception:
        geojson_path = out_path.with_suffix(".geojson")
        geojson_path.write_text(
            '{"type":"FeatureCollection","features":[]}',
            encoding="utf-8",
        )
        log_fn(f"⚠  Empty hotspot layer written as GeoJSON → {geojson_path.name}.")
        return geojson_path


def _write_hotspot_geojson(out_path: Path,
                            cfg: "PipelineConfig",
                            window: dict,
                            grid_shape: tuple,
                            lat_centers: "np.ndarray",
                            lon_centers: "np.ndarray",
                            composites: dict,
                            scalar_keys: List[str],
                            is_hotspot_ch4: Optional["np.ndarray"],
                            is_hotspot_xco2: Optional["np.ndarray"],
                            insufficient: "np.ndarray") -> Optional[Path]:
    """Fallback GeoJSON writer used when geopandas is unavailable.

    Matches the ``_export_fc_to_gpkg`` pattern of degrading to GeoJSON with
    a ``.geojson`` suffix so the Results tab's file loader still finds it.
    """
    try:
        rows, cols = grid_shape
        features = []
        for r in range(rows):
            for c in range(cols):
                props = {
                    "window_name":      window.get("name", ""),
                    "row":              int(r),
                    "col":              int(c),
                    "lat":              float(lat_centers[r]),
                    "lon":              float(lon_centers[c]),
                    "is_hotspot_ch4":   bool(is_hotspot_ch4[r, c]) if is_hotspot_ch4 is not None else False,
                    "is_hotspot_xco2":  bool(is_hotspot_xco2[r, c]) if is_hotspot_xco2 is not None else False,
                    "insufficient_data": bool(insufficient[r, c]),
                }
                for k in scalar_keys:
                    val = composites[k][r, c]
                    if isinstance(val, (np.floating, np.integer)):
                        val = val.item()
                    # GeoJSON cannot represent NaN in strict mode → null.
                    if isinstance(val, float) and not math.isfinite(val):
                        val = None
                    props[k] = val
                features.append({
                    "type":       "Feature",
                    "geometry":   {"type": "Point",
                                   "coordinates": [float(lon_centers[c]),
                                                    float(lat_centers[r])]},
                    "properties": props,
                })

        out_path = Path(out_path).with_suffix(".geojson")
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(
            json.dumps({"type": "FeatureCollection", "features": features}),
            encoding="utf-8",
        )
        return out_path
    except Exception as e:
        log.warning("_write_hotspot_geojson failed: %s", e)
        return None


# ═══════════════════════════════════════════════════════════════════════════
# Export helpers
# ═══════════════════════════════════════════════════════════════════════════

def _export_image_to_drive_or_local(ee, image, aoi, local_path: Path,
                                     description: str, scale: float,
                                     prog: Optional[ProgressFn] = None):
    """
    Export a GEE Image to a local GeoTIFF via getDownloadURL.
    Falls back to a GEE Drive export task for large AOIs that time out.
    All errors are surfaced via prog() so they appear in the pipeline log.
    """
    import urllib.request
    local_path = Path(local_path)
    def _p(msg): prog(50, msg) if prog else log.info(msg)

    try:
        _p(f"Requesting download URL for {description} …")
        url = image.getDownloadURL({
            "name":        description,
            "region":      aoi,
            "scale":       scale,
            "format":      "GEO_TIFF",
            "filePerBand": False,
        })
        _p(f"Downloading {local_path.name} …")
        urllib.request.urlretrieve(url, str(local_path))
        _p(f"✓  Saved {local_path.name}  ({local_path.stat().st_size // 1024} KB)")
        return local_path

    except Exception as e:
        _p(f"⚠  Direct download failed: {e}")
        _p("Submitting GEE Drive export task as fallback …")
        try:
            task = ee.batch.Export.image.toDrive(
                image=image,
                description=description,
                region=aoi,
                scale=scale,
                fileFormat="GeoTIFF",
            )
            task.start()
            _p(f"✓  Drive export task submitted: '{description}'. "
               f"Check https://code.earthengine.google.com/tasks")
            placeholder = local_path.with_suffix(".task_submitted.txt")
            placeholder.write_text(
                f"GEE Drive export task submitted at {datetime.now().isoformat()}\n"
                f"Description: {description}\n"
                f"Check https://code.earthengine.google.com/tasks\n"
            )
            return placeholder
        except Exception as e2:
            _p(f"❌  Drive export also failed: {e2}")
            raise RuntimeError(
                f"Could not export {description}. "
                f"Direct: {e}. Drive: {e2}"
            ) from e2


def _export_fc_to_gpkg(ee, fc, local_path: Path,
                        prog: Optional[ProgressFn] = None):
    """
    Export a FeatureCollection to a local GeoPackage via getDownloadURL.
    Falls back to GeoJSON if geopandas is unavailable.
    All errors are raised so they surface in the pipeline log.
    """
    import urllib.request
    local_path = Path(local_path)
    def _p(msg): prog(70, msg) if prog else log.info(msg)

    _p("Requesting hotspot GeoJSON download URL from GEE …")
    # Raises immediately on GEE error — caller sees the real message.
    url = fc.getDownloadURL(filetype="GeoJSON", filename="ghg_hotspots")

    geojson_path = local_path.with_suffix(".geojson")
    _p("Downloading hotspot GeoJSON …")
    urllib.request.urlretrieve(url, str(geojson_path))

    try:
        import geopandas as gpd
        gdf = gpd.read_file(str(geojson_path))
        _p(f"Hotspot features loaded: {len(gdf)} rows.")
        gdf.to_file(str(local_path), driver="GPKG")
        geojson_path.unlink(missing_ok=True)
        _p(f"✓  Hotspot GeoPackage saved → {local_path.name}")
        return local_path
    except ImportError:
        _p("⚠  geopandas not installed — hotspots saved as GeoJSON instead.")
        return geojson_path


def _write_soc_points(records: list, wb_correction: bool, out_path: Path):
    """
    Write SOC/SIC field points to a GeoPackage.
    Applies Walkley-Black x1.334 correction if requested.
    """
    out_path = Path(out_path)
    corrected = []
    for r in records:
        row = dict(r)
        if wb_correction and "soc" in row and row["soc"] not in (None, ""):
            try:
                row["soc_corrected"] = float(row["soc"]) * WALKLEY_BLACK_CORRECTION
            except (ValueError, TypeError):
                row["soc_corrected"] = None
        corrected.append(row)

    try:
        import geopandas as gpd
        from shapely.geometry import Point
        gdf = gpd.GeoDataFrame(
            corrected,
            geometry=[Point(float(r["lon"]), float(r["lat"])) for r in corrected],
            crs="EPSG:4326"
        )
        gdf.to_file(str(out_path), driver="GPKG")
        log.info("SOC points saved → %s (%d records)", out_path, len(corrected))
    except ImportError:
        # fallback: write CSV
        import csv
        csv_path = out_path.with_suffix(".csv")
        if corrected:
            with open(csv_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=corrected[0].keys())
                writer.writeheader()
                writer.writerows(corrected)
        log.info("SOC points saved as CSV (geopandas unavailable) → %s", csv_path)
        return csv_path

    return out_path


def _validate_caaqms(caaqms_csv: str, hotspot_path, out_path: Path):
    """
    Basic uncertainty analysis: load CAAQMS CSV, compute summary stats
    per pollutant, write uncertainty table CSV.

    Full spatial matching to hotspot grid requires geopandas.
    Returns a dict of summary statistics.
    """
    import csv, math
    out_path = Path(out_path)

    POLLUTANTS = [
        "PM10", "PM2.5", "SO2", "NO", "NO2", "NOX",
        "NH3", "CO", "O3", "Benzene", "Toluene",
        "Ethylbenzene", "MP_XYLENE", "O_XYLENE"
    ]

    stats = {}
    rows_by_pollutant = {p: [] for p in POLLUTANTS}

    with open(caaqms_csv, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        all_rows = list(reader)

    for row in all_rows:
        for p in POLLUTANTS:
            val = row.get(p, "")
            if val not in ("", "NA", "NaN", "null", "NULL", "--"):
                try:
                    rows_by_pollutant[p].append(float(val))
                except ValueError:
                    pass

    summary_rows = []
    for p in POLLUTANTS:
        vals = rows_by_pollutant[p]
        if not vals:
            continue
        n    = len(vals)
        mean = sum(vals) / n
        sd   = math.sqrt(sum((v - mean) ** 2 for v in vals) / n) if n > 1 else 0
        mn   = min(vals)
        mx   = max(vals)
        # coefficient of variation as proxy uncertainty metric
        cv   = (sd / mean * 100) if mean != 0 else 0
        stats[p] = {"n": n, "mean": round(mean, 3), "sd": round(sd, 3),
                    "min": round(mn, 3), "max": round(mx, 3),
                    "cv_pct": round(cv, 2)}
        summary_rows.append({
            "pollutant": p, "n": n,
            "mean": round(mean, 3), "sd": round(sd, 3),
            "min": round(mn, 3), "max": round(mx, 3),
            "cv_pct": round(cv, 2),
            "uncertainty_note": "spatial matching pending GEE hotspot export"
        })

    if summary_rows:
        with open(out_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=summary_rows[0].keys())
            writer.writeheader()
            writer.writerows(summary_rows)
        log.info("CAAQMS uncertainty summary → %s", out_path)

    return stats


# ═══════════════════════════════════════════════════════════════════════════
# Metadata writers
# ═══════════════════════════════════════════════════════════════════════════

def _write_summary(cfg: PipelineConfig, results: dict, path: Path):
    lines = [
        "GHG Mapper — Run Summary",
        "=" * 50,
        f"Start time : {results.get('start_time', '—')}",
        f"End time   : {results.get('end_time', '—')}",
        f"Status     : {results.get('status', '—')}",
        "",
        "AOI",
        f"  West  : {cfg.aoi_west}°E",
        f"  East  : {cfg.aoi_east}°E",
        f"  South : {cfg.aoi_south}°N",
        f"  North : {cfg.aoi_north}°N",
        "",
        f"Period     : {cfg.start_date} → {cfg.end_date}",
        f"Grid res   : {cfg.grid_res}° (~{cfg.grid_res*111:.0f} km)",
        f"GEE project: {cfg.gee_project}",
        "",
        "Satellites used",
        f"  TROPOMI CH4       : {'✓' if cfg.use_tropomi else '—'}",
        f"  OCO-2 XCO2        : {'✓' if cfg.use_oco2 else '—'}",
        f"  OCO-3 XCO2        : {'✓' if cfg.use_oco3 else '—'}",
        f"  GOSAT XCO2 (ACOS) : {'✓' if cfg.use_gosat else '—'}",
        f"  GOSAT XCH4 (NIES) : {'✓' if (cfg.use_gosat and cfg.nies_user) else '—'}",
        "",
        "Output files",
    ]
    for k, v in results.get("files", {}).items():
        lines.append(f"  {k:<22}: {v}")

    if results.get("errors"):
        lines += ["", "Errors", "-" * 30]
        for e in results["errors"]:
            lines.append(e)

    path.write_text("\n".join(lines), encoding="utf-8")
    log.info("Run summary → %s", path)


def _write_config(cfg: PipelineConfig, results: dict, path: Path):
    payload = {
        "pipeline_version": "0.3.0",
        "generated": datetime.now().isoformat(),
        "config": asdict(cfg),
        "output_files": results.get("files", {}),
        "stats": results.get("stats", {}),
    }
    path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    log.info("Run config → %s", path)
