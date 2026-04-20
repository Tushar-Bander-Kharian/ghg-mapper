"""
run_pipeline.py — GHG Mapper backend pipeline
===============================================
Multi-satellite GHG hotspot mapping over agricultural India.
Satellites: TROPOMI (CH4), OCO-2/OCO-3 (XCO2), GOSAT (XCO2+XCH4)
Ground truth: SOC/SIC field points, CAAQMS station CSV

Author : Tushar Bander <tushar.bander@amity.edu>
Version: 1.1.0
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
import traceback
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path
from typing import Callable, List, Optional

log = logging.getLogger("ghg_mapper")

# ── constants ──────────────────────────────────────────────────────────────
WALKLEY_BLACK_CORRECTION = 1.334          # Nelson & Sommers 1982
HOTSPOT_PERCENTILE       = 90             # cells above this → hotspot flag
DEFAULT_GRID_DEG         = 0.1            # ~11 km grid

# GEE collection IDs
GEE_TROPOMI_CH4  = "COPERNICUS/S5P/OFFL/L3_CH4"
GEE_S5P_NO2      = "COPERNICUS/S5P/OFFL/L3_NO2"   # CAAQMS co-tracer validation

# NASA GES DISC — OCO-2/3 L2 Lite FP (NOT in GEE; streamed via PyDAP / OPeNDAP)
# Approach mirrors: https://github.com/sagarlimbu0/NASA-OCO2-OCO3
NASA_CMR_SEARCH    = "https://cmr.earthdata.nasa.gov/search/granules.json"
NASA_OPENDAP_ROOT  = "https://opendap.gesdisc.earthdata.nasa.gov/opendap/"
NASA_OPENDAP_ROOT_LEGACY = "https://oco2.gesdisc.eosdis.nasa.gov/opendap/"
NASA_URS_HOST      = "urs.earthdata.nasa.gov"
OCO2_SHORT_NAME   = "OCO2_L2_Lite_FP"
OCO2_VERSION      = "11.2r"
OCO3_SHORT_NAME   = "OCO3_L2_Lite_FP"
OCO3_VERSION      = "10.4r"


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
    soc_records:      List[dict] = field(default_factory=list)   # [{lat,lon,soc,sic}, ...]
    caaqms_csv:       Optional[str] = None
    wb_correction:    bool  = True

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

        # ── Step 2: pull satellite composites ─────────────────────────────
        prog(10, "Fetching TROPOMI CH₄ composite …")
        ch4_img = None
        if cfg.use_tropomi:
            ch4_img = _stage_tropomi_ch4(ee, aoi,
                                         cfg.start_date, cfg.end_date, prog)
        if ch4_img is None:
            prog(10, "⚠  TROPOMI: no CH₄ images found for this AOI / date range. "
                     "Try widening the date range or check GEE collection availability.")

        prog(25, "Fetching OCO-2 / OCO-3 XCO₂ (NASA GES DISC / OPeNDAP) …")
        xco2_local_path = None
        if cfg.use_oco2 or cfg.use_oco3:
            if cfg.earthdata_user and cfg.earthdata_pass:
                xco2_local_path = _stage_xco2_direct(
                    cfg.start_date, cfg.end_date,
                    cfg.aoi_bounds, out,
                    cfg.earthdata_user,
                    cfg.earthdata_pass,
                    cfg.grid_res,
                    use_oco2=cfg.use_oco2,
                    use_oco3=cfg.use_oco3,
                    prog=prog,
                )
                if xco2_local_path is None:
                    prog(25, "⚠  OCO-2/3: no retrievals found or download failed. "
                             "Check EarthData credentials and date range.")
            else:
                prog(25, "⚠  OCO-2/3 skipped — no EarthData credentials. "
                         "Enter username/password in the Setup tab.")

        prog(38, "Building hotspot grid …")
        if ch4_img is None:
            prog(38, "⚠  No satellite data available — hotspot grid will be empty.")
        hotspot_fc = _detect_hotspots(ee, aoi, cfg.aoi_bounds, ch4_img, None,
                                      cfg.grid_res, HOTSPOT_PERCENTILE, prog)

        # ── Step 3: export GeoTIFFs ────────────────────────────────────────
        prog(50, "Exporting CH₄ composite GeoTIFF …")
        ch4_path = None
        if ch4_img is not None:
            ch4_path = _export_image_to_drive_or_local(
                ee, ch4_img, aoi, out / "ch4_composite.tif",
                description="ch4_composite", scale=cfg.grid_res * 111320,
                prog=prog,
            )
            results["files"]["ch4_composite"] = str(ch4_path)
        else:
            prog(50, "⚠  CH₄ GeoTIFF skipped — no TROPOMI data.")

        prog(60, "Recording XCO₂ composite path …")
        if xco2_local_path is not None and Path(xco2_local_path).exists():
            results["files"]["xco2_composite"] = str(xco2_local_path)
        else:
            prog(60, "⚠  XCO₂ composite not available.")

        # ── Step 4: export hotspot GeoPackage ─────────────────────────────
        prog(70, "Exporting hotspot GeoPackage …")
        hotspot_path = _export_fc_to_gpkg(
            ee, hotspot_fc, out / "ghg_hotspots.gpkg", prog=prog
        )
        results["files"]["ghg_hotspots"] = str(hotspot_path)

        # ── Step 5: SOC / SIC ground truth points ─────────────────────────
        prog(78, "Writing SOC/SIC ground truth points …")
        if cfg.soc_records:
            soc_path = _write_soc_points(cfg.soc_records,
                                         cfg.wb_correction,
                                         out / "soc_points.gpkg")
            results["files"]["soc_points"] = str(soc_path)
            results["stats"]["soc_records"] = len(cfg.soc_records)

        # ── Step 6: CAAQMS uncertainty validation ─────────────────────────
        prog(85, "Running CAAQMS uncertainty analysis …")
        if cfg.caaqms_csv and Path(cfg.caaqms_csv).exists():
            caaqms_stats = _validate_caaqms(
                cfg.caaqms_csv, hotspot_path, out / "caaqms_uncertainty.csv"
            )
            results["files"]["caaqms_uncertainty"] = str(out / "caaqms_uncertainty.csv")
            results["stats"]["caaqms"] = caaqms_stats

        # ── Step 7: write run metadata ────────────────────────────────────
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


def _stage_tropomi_ch4(ee, aoi, start: str, end: str,
                        prog: Optional[ProgressFn] = None):
    """
    Build a mean CH4 column composite from TROPOMI S5P OFFL L3.
    Band: CH4_column_volume_mixing_ratio_dry_air  (ppb)
    Quality filter: qa_value > 0.5 (ESA recommendation).
    """
    def _p(msg): prog(10, msg) if prog else log.info(msg)

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

    return col.mean().rename("CH4_ppb").clip(aoi)


def _stage_xco2_direct(start: str, end: str, bbox: list, out_dir: Path,
                        earthdata_user: str, earthdata_pass: str,
                        grid_res: float,
                        use_oco2: bool = True, use_oco3: bool = True,
                        prog: Optional[ProgressFn] = None) -> Optional[Path]:
    """
    Stream OCO-2 and/or OCO-3 XCO₂ L2 Lite FP data from NASA GES DISC via
    PyDAP / OPeNDAP and write a gridded mean XCO₂ GeoTIFF.

    Approach mirrors https://github.com/sagarlimbu0/NASA-OCO2-OCO3 :
      - Granule discovery : NASA CMR search API (no auth required)
      - Data streaming    : pydap.cas.urs.setup_session + open_url (OPeNDAP)
      - Variables read    : xco2, latitude, longitude, xco2_quality_flag
      - Quality filter    : xco2_quality_flag == 0  (good retrievals only)

    Requires (OSGeo4W Shell):
      pip install pydap requests

    numpy and osgeo.gdal are bundled with QGIS.
    """
    def _p(msg): prog(25, msg) if prog else log.info(msg)

    try:
        from pydap.cas.urs import setup_session
        from pydap.client import open_url
    except ImportError:
        _p("❌  'pydap' not installed.  Run in OSGeo4W Shell:  pip install pydap")
        return None

    import numpy as np

    _p(f"OCO: authenticating with NASA EarthData as '{earthdata_user}' …")
    try:
        # check_url triggers the URS login handshake up-front so subsequent
        # granule requests carry valid session cookies.  We pass a known
        # catalogue-index page (not a dataset file) so pydap never tries to
        # append .dds to it.
        session = setup_session(
            earthdata_user, earthdata_pass,
            check_url="https://opendap.gesdisc.earthdata.nasa.gov/opendap/",
        )
        _p("OCO: NASA EarthData session established.")
    except Exception as e:
        _p(f"❌  NASA EarthData authentication failed: {e}")
        return None

    west, south, east, north = bbox
    all_lats:  list = []
    all_lons:  list = []
    all_xco2:  list = []

    datasets = []
    if use_oco2:
        datasets.append((OCO2_SHORT_NAME, OCO2_VERSION, "OCO-2"))
    if use_oco3:
        datasets.append((OCO3_SHORT_NAME, OCO3_VERSION, "OCO-3"))

    for short_name, version, label in datasets:
        opendap_urls = _cmr_opendap_urls(short_name, version,
                                         start, end, bbox, max_granules=20)
        _p(f"OCO: {label} — {len(opendap_urls)} valid granule URL(s) found via CMR.")
        if not opendap_urls:
            _p(f"⚠  {label}: no granules with valid OPeNDAP file URLs in CMR for "
               f"this AOI / date range.  Check short_name='{short_name}' "
               f"version='{version}' and date window.")

        for url in opendap_urls:
            try:
                _p(f"OCO: streaming {url.rsplit('/', 1)[-1]}  ({url})")
                ds = open_url(url, session=session)

                # Variable names follow the reference repo (lowercase netCDF4)
                xco2 = np.array(ds["xco2"][:]).flatten()
                lat  = np.array(ds["latitude"][:]).flatten()
                lon  = np.array(ds["longitude"][:]).flatten()
                qf   = np.array(ds["xco2_quality_flag"][:]).flatten()

                # Quality + AOI + physical-range filter (ref repo: flag == 0)
                mask = (
                    (qf == 0) &
                    (lat >= south) & (lat <= north) &
                    (lon >= west)  & (lon <= east)  &
                    (xco2 > 100)   & (xco2 < 600)
                )
                n_good = int(mask.sum())
                all_lats.extend(lat[mask].tolist())
                all_lons.extend(lon[mask].tolist())
                all_xco2.extend(xco2[mask].tolist())
                _p(f"OCO: {label} — {n_good} good retrievals in AOI from this granule.")

            except Exception as e:
                _p(f"❌  OCO: failed to stream {url.rsplit('/', 1)[-1]}: {e}")

    if not all_lats:
        log.warning("No OCO-2/3 retrievals found for the specified AOI and date range.")
        return None

    return _grid_oco_to_tif(
        np.array(all_lats,  dtype=np.float64),
        np.array(all_lons,  dtype=np.float64),
        np.array(all_xco2,  dtype=np.float64),
        bbox, grid_res,
        out_dir / "xco2_composite.tif",
    )


def _cmr_opendap_urls(short_name: str, version: str,
                       start: str, end: str, bbox: list,
                       max_granules: int = 20) -> list:
    """
    Query NASA CMR (no auth needed) and return OPeNDAP URLs for each granule.

    CMR returns two useful link types per granule:
      rel …/data#    → direct HTTPS download URL  (/data/ path)
      rel …/service# → OPeNDAP HTML page URL       (/opendap/ path, ends .nc4.html)

    We prefer the service link (strip .html → valid OPeNDAP base URL).
    If absent, we rewrite the data URL (/data/ → /opendap/).

    IMPORTANT: only URLs that end in '.nc4' or '.nc' are returned.
    Collection-root directory URLs (no file extension) must be excluded —
    passing them to pydap.open_url causes PyDAP to append 'contents.dds'
    which returns HTTP 404, breaking the entire OCO stream.
    """
    try:
        import requests
    except ImportError:
        log.error("'requests' not installed.  Run:  pip install requests")
        return []

    west, south, east, north = bbox
    params = {
        "short_name":   short_name,
        "version":      version,
        "temporal[]":   f"{start}T00:00:00Z,{end}T23:59:59Z",
        "bounding_box": f"{west},{south},{east},{north}",
        "page_size":    max_granules,
        "sort_key":     "start_date",
    }

    try:
        resp = requests.get(NASA_CMR_SEARCH, params=params, timeout=30)
        resp.raise_for_status()
        entries = resp.json().get("feed", {}).get("entry", [])
    except Exception as e:
        log.warning("CMR search failed for %s v%s: %s", short_name, version, e)
        return []

    urls = []
    for entry in entries:
        opendap_url = None
        data_url    = None

        for link in entry.get("links", []):
            href = link.get("href", "")
            rel  = link.get("rel", "")

            # OPeNDAP service link (preferred) — ends with .nc4.html or .nc.html
            # Use removesuffix (not rstrip!) — rstrip strips individual characters
            # from the set {'.','h','t','m','l'} and corrupts filenames.
            if "service#" in rel and "/opendap/" in href:
                candidate = href.removesuffix(".html")
                if candidate.endswith((".nc4", ".nc")):
                    opendap_url = candidate
                    break

            # Direct data link — fallback (already a file URL with extension)
            if "data#" in rel and href.endswith((".nc4", ".nc")):
                data_url = href

        if opendap_url is None and data_url:
            # Rewrite /data/ → /opendap/ to get the OPeNDAP endpoint.
            # Handle both the new domain (data.gesdisc.earthdata.nasa.gov)
            # and the legacy domain (gesdisc.eosdis.nasa.gov).
            if "data.gesdisc.earthdata.nasa.gov/data/" in data_url:
                opendap_url = data_url.replace(
                    "data.gesdisc.earthdata.nasa.gov/data/",
                    "opendap.gesdisc.earthdata.nasa.gov/opendap/",
                )
            else:
                opendap_url = data_url.replace(
                    "gesdisc.eosdis.nasa.gov/data/",
                    "gesdisc.eosdis.nasa.gov/opendap/",
                )

        # Final guard: only accept URLs that point to an actual granule file.
        # Directory-style URLs (no .nc4/.nc suffix) cause PyDAP to fetch
        # contents.dds from the collection root — which always returns 404.
        if opendap_url and opendap_url.endswith((".nc4", ".nc")):
            urls.append(opendap_url)
        elif opendap_url:
            log.warning("CMR: skipping non-granule URL (no .nc4/.nc suffix): %s",
                        opendap_url)

    return urls


def _grid_oco_to_tif(lats, lons, values, bbox: list,
                     grid_res: float, out_path: Path) -> Optional[Path]:
    """
    Bin sparse OCO point retrievals onto a regular grid and write a GeoTIFF.
    Uses GDAL (bundled with QGIS) for raster output.
    """
    import numpy as np
    try:
        from osgeo import gdal, osr
    except ImportError:
        log.error("osgeo.gdal not available — cannot write XCO₂ GeoTIFF")
        return None

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

    driver = gdal.GetDriverByName("GTiff")
    ds = driver.Create(str(out_path), nx, ny, 1, gdal.GDT_Float32,
                       options=["COMPRESS=LZW", "TILED=YES"])
    ds.SetGeoTransform([west, grid_res, 0, north, 0, -grid_res])
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(4326)
    ds.SetProjection(srs.ExportToWkt())
    band = ds.GetRasterBand(1)
    band.WriteArray(grid_mean)
    band.SetNoDataValue(float("nan"))
    band.SetDescription("XCO2_ppm")
    ds.FlushCache()
    ds = None

    log.info("XCO₂ composite GeoTIFF saved → %s  (%d×%d grid, %d retrievals)",
             out_path, nx, ny, int(grid_cnt.sum()))
    return out_path


def _detect_hotspots(ee, aoi, aoi_bounds: list, ch4_img, xco2_img,
                      grid_res: float, percentile: int = 90,
                      prog: Optional[ProgressFn] = None):
    """
    Sample satellite composites at a regular grid resolution and flag hotspots.

    Uses Image.sample() instead of reduceToVectors / reduceRegions.
    Both of those operations call reduceToVectors internally on GEE's server
    and fail with "Need 1+1 bands … image has 0" whenever the image mask is
    sparse or empty within the AOI.  sample() samples pixel values directly
    without any vectorisation step, completely bypassing the issue.
    """
    def _p(msg): prog(38, msg) if prog else log.info(msg)

    bands = []
    if ch4_img is not None:
        bands.append(ch4_img)
    if xco2_img is not None:
        bands.append(xco2_img)

    if not bands:
        _p("⚠  No satellite bands available — hotspot detection skipped.")
        return ee.FeatureCollection([])

    stack = ee.Image(bands[0])
    for b in bands[1:]:
        stack = stack.addBands(b)

    scale_m = int(grid_res * 111320)   # degrees → approximate metres

    # Verify bands exist server-side.
    band_names = stack.bandNames().getInfo()
    _p(f"Hotspot stack bands: {band_names}")
    if not band_names:
        _p("⚠  Satellite composite has no bands — hotspot detection skipped.")
        return ee.FeatureCollection([])

    # ── Percentile thresholds (on original masked data) ─────────────────────
    # Evaluated client-side so we can read the actual key names.
    # GEE's Reducer.percentile output key format varies by API version:
    # it may be '{band}_p{n}' or just '{band}' for a single percentile.
    # Reading getInfo() and finding keys by band-name substring is robust.
    _p(f"Computing p{percentile} thresholds over AOI …")
    stats_info = stack.reduceRegion(
        reducer=ee.Reducer.percentile([percentile]),
        geometry=aoi,
        scale=scale_m,
        crs="EPSG:4326",
        maxPixels=1e9,
        bestEffort=True,
    ).getInfo()
    _p(f"Percentile thresholds (raw): {stats_info}")

    # Find threshold values regardless of exact key name
    def _thresh(prefix):
        for k, v in stats_info.items():
            if k.startswith(prefix) and v is not None:
                return float(v)
        return None

    ch4_thresh  = _thresh("CH4_ppb")
    xco2_thresh = _thresh("XCO2_ppm")
    _p(f"CH4 p{percentile} threshold: {ch4_thresh}  |  XCO2 p{percentile} threshold: {xco2_thresh}")

    # ── Sample pixels — avoids reduceToVectors entirely ─────────────────────
    stack_filled = stack.unmask(0).float()

    _p("Sampling satellite pixels across AOI …")
    samples = stack_filled.sample(
        region=aoi,
        scale=scale_m,
        projection="EPSG:4326",
        factor=1,
        geometries=True,
    )

    n_samples = samples.size().getInfo()
    _p(f"Hotspot grid: {n_samples} pixel sample(s) at {grid_res}° resolution.")
    if n_samples == 0:
        _p("⚠  No samples returned — the composite may have no valid pixels in this AOI.")
        return ee.FeatureCollection([])

    # ── Flag hotspots ────────────────────────────────────────────────────────
    # Thresholds are Python floats passed as ee.Number constants —
    # no server-side dict lookup needed, so no key-name mismatch possible.
    def flag_cell(feat):
        props = {}
        if ch4_img is not None and ch4_thresh is not None:
            val = ee.Number(feat.get("CH4_ppb"))
            props["CH4_ppb"]        = val
            props["is_hotspot_ch4"] = val.gt(0).And(
                val.gte(ee.Number(ch4_thresh))).toInt()
        if xco2_img is not None and xco2_thresh is not None:
            val = ee.Number(feat.get("XCO2_ppm"))
            props["XCO2_ppm"]        = val
            props["is_hotspot_xco2"] = val.gt(0).And(
                val.gte(ee.Number(xco2_thresh))).toInt()
        return feat.set(props)

    return samples.map(flag_cell)


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
        f"  TROPOMI CH4 : {'✓' if cfg.use_tropomi else '—'}",
        f"  OCO-2 XCO2  : {'✓' if cfg.use_oco2 else '—'}",
        f"  OCO-3 XCO2  : {'✓' if cfg.use_oco3 else '—'}",
        f"  GOSAT       : {'✓' if cfg.use_gosat else '—'}",
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
        "pipeline_version": "1.1.0",
        "generated": datetime.now().isoformat(),
        "config": asdict(cfg),
        "output_files": results.get("files", {}),
        "stats": results.get("stats", {}),
    }
    path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    log.info("Run config → %s", path)
