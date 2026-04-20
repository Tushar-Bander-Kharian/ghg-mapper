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

# NASA GES DISC — OCO-2/3 and GOSAT ACOS XCO2 (direct HTTPS download + h5py)
# Approach mirrors: https://github.com/sagarlimbu0/NASA-OCO2-OCO3
NASA_CMR_SEARCH   = "https://cmr.earthdata.nasa.gov/search/granules.json"
NASA_OPENDAP_ROOT = "https://oco2.gesdisc.eosdis.nasa.gov/opendap/"
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

        prog(25, "Fetching OCO-2 / OCO-3 / GOSAT XCO₂ (NASA GES DISC) …")
        xco2_local_path = None
        if cfg.use_oco2 or cfg.use_oco3 or cfg.use_gosat:
            if cfg.earthdata_user and cfg.earthdata_pass:
                xco2_local_path = _stage_xco2_direct(
                    cfg.start_date, cfg.end_date,
                    cfg.aoi_bounds, out,
                    cfg.earthdata_user,
                    cfg.earthdata_pass,
                    cfg.grid_res,
                    use_oco2=cfg.use_oco2,
                    use_oco3=cfg.use_oco3,
                    use_gosat=cfg.use_gosat,
                    prog=prog,
                )
                if xco2_local_path is None:
                    prog(25, "⚠  OCO-2/3/GOSAT: no retrievals found or download failed. "
                             "Check EarthData credentials and date range.")
            else:
                prog(25, "⚠  OCO-2/3/GOSAT skipped — no EarthData credentials. "
                         "Enter username/password in the Setup tab.")

        prog(30, "Fetching GOSAT XCH₄ (NIES portal) …")
        gosat_ch4_path = None
        if cfg.use_gosat and cfg.nies_user and cfg.nies_pass:
            gosat_ch4_path = _stage_gosat_ch4_nies(
                cfg.start_date, cfg.end_date,
                cfg.aoi_bounds, out,
                cfg.nies_user, cfg.nies_pass,
                cfg.grid_res, prog=prog,
            )
            if gosat_ch4_path is None:
                prog(30, "⚠  GOSAT XCH₄: download failed or no data in AOI. "
                         "Check NIES credentials.")
        elif cfg.use_gosat:
            prog(30, "⚠  GOSAT XCH₄ skipped — no NIES credentials. "
                     "Register at https://prdct.gosat-2.nies.go.jp and enter credentials.")

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
                        use_gosat: bool = True,
                        prog: Optional[ProgressFn] = None) -> Optional[Path]:
    """
    Download OCO-2 and/or OCO-3 XCO₂ L2 Lite FP granules from NASA GES DISC
    via direct HTTPS and write a gridded mean XCO₂ GeoTIFF.

    OPeNDAP streaming via pydap was abandoned because the session cookies
    obtained from setup_session do not persist into the .dods data-fetch
    requests, causing every granule to return an OAuth redirect HTML page that
    pydap fails to parse ("not enough values to unpack").

    New approach (mirrors reference repo credential handling):
      - Credentials      : written to ~/.netrc for urs.earthdata.nasa.gov
      - Auth session     : pydap.cas.urs.setup_session with check_url set to
                           the first direct data URL so URS cookies are scoped
                           to data.gesdisc.earthdata.nasa.gov
      - Granule download : session.get(direct_data_url) → temp .nc4 file
      - Variables read   : h5py (primary) → netCDF4 (fallback)
      - Quality filter   : xco2_quality_flag == 0

    Requires (OSGeo4W Shell):
      pip install pydap requests h5py
    """
    def _p(msg): prog(25, msg) if prog else log.info(msg)

    try:
        import requests as _requests
    except ImportError:
        _p("❌  'requests' not installed.  Run:  pip install requests")
        return None

    import numpy as np
    import tempfile
    import os

    _ensure_earthdata_netrc(earthdata_user, earthdata_pass)

    datasets = []
    if use_oco2:
        datasets.append((OCO2_SHORT_NAME, OCO2_VERSION, "OCO-2"))
    if use_oco3:
        datasets.append((OCO3_SHORT_NAME, OCO3_VERSION, "OCO-3"))
    if use_gosat:
        # GOSAT ACOS L2 Lite — same GES DISC auth as OCO; quality flag field
        # is "quality_flag" (not "xco2_quality_flag").
        datasets.append((GOSAT_SHORT_NAME, GOSAT_VERSION, "GOSAT"))

    # Collect direct data URLs from CMR (no OPeNDAP rewriting).
    labelled_urls: list = []
    for short_name, version, label in datasets:
        urls = _cmr_direct_data_urls(short_name, version, start, end, bbox,
                                     max_granules=8)
        _p(f"OCO: {label} — {len(urls)} granule(s) found via CMR.")
        if not urls:
            _p(f"⚠  {label}: no granules found in CMR for this AOI / date range. "
               f"(short_name={short_name!r} version={version!r})")
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

    west, south, east, north = bbox
    all_lats: list = []
    all_lons: list = []
    all_xco2: list = []

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

            xco2_arr, lat_arr, lon_arr, qf_arr = _read_nc4_oco_vars(tmp_path)

            mask = (
                (qf_arr == 0) &
                (lat_arr >= south) & (lat_arr <= north) &
                (lon_arr >= west)  & (lon_arr <= east)  &
                (xco2_arr > 100)   & (xco2_arr < 600)
            )
            n_good = int(mask.sum())
            all_lats.extend(lat_arr[mask].tolist())
            all_lons.extend(lon_arr[mask].tolist())
            all_xco2.extend(xco2_arr[mask].tolist())
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

    return _grid_oco_to_tif(
        np.array(all_lats,  dtype=np.float64),
        np.array(all_lons,  dtype=np.float64),
        np.array(all_xco2,  dtype=np.float64),
        bbox, grid_res,
        out_dir / "xco2_composite.tif",
    )


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
        for link in entry.get("links", []):
            href = link.get("href", "")
            rel  = link.get("rel", "")
            if "data#" in rel and href.endswith((".nc4", ".nc")):
                urls.append(href)
                break
    return urls


def _read_nc4_oco_vars(path: str):
    """
    Read xco2, latitude, longitude, quality_flag from an OCO-2/3 or GOSAT NC4 file.
    Handles two quality-flag field names:
      - OCO-2/3  : "xco2_quality_flag"
      - GOSAT ACOS: "quality_flag"
    Tries h5py first (bundled with QGIS/OSGeo4W), then netCDF4.
    Returns four flat numpy arrays (float64, float64, float64, int32).
    """
    import numpy as np

    def _pick_qf(keys):
        for name in ("xco2_quality_flag", "quality_flag"):
            if name in keys:
                return name
        raise KeyError(f"No quality-flag field found. Available: {list(keys)}")

    try:
        import h5py
        with h5py.File(path, "r") as f:
            qf_key = _pick_qf(f.keys())
            xco2 = np.array(f["xco2"]).flatten().astype(np.float64)
            lat  = np.array(f["latitude"]).flatten().astype(np.float64)
            lon  = np.array(f["longitude"]).flatten().astype(np.float64)
            qf   = np.array(f[qf_key]).flatten().astype(np.int32)
        return xco2, lat, lon, qf
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
        return xco2, lat, lon, qf
    except ImportError:
        pass

    # GDAL fallback — always bundled with QGIS/OSGeo4W, no pip install needed.
    try:
        from osgeo import gdal as _gdal
        import numpy as _np2

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
        return xco2, lat, lon, qf
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
            # CMR data URLs look like:
            #   data.gesdisc.earthdata.nasa.gov/data/OCO2_DATA/OCO2_L2_Lite_FP.11.2r/YYYY/file.nc4
            # The old OPeNDAP server omits the intermediate *_DATA/ collection dir:
            #   oco2.gesdisc.eosdis.nasa.gov/opendap/OCO2_L2_Lite_FP.11.2r/YYYY/file.nc4
            # Strip host+/data/ prefix, remove the leading *_DATA/ segment, then
            # prepend the OPeNDAP root.
            import re as _re
            for _pfx in ("data.gesdisc.earthdata.nasa.gov/data/",
                         "gesdisc.eosdis.nasa.gov/data/"):
                if _pfx in data_url:
                    _tail = data_url.split(_pfx, 1)[1]
                    _tail = _re.sub(r'^[A-Z0-9]+_DATA/', '', _tail)
                    opendap_url = NASA_OPENDAP_ROOT + _tail
                    break

        # Final guard: only accept URLs that point to an actual granule file.
        # Directory-style URLs (no .nc4/.nc suffix) cause PyDAP to fetch
        # contents.dds from the collection root — which always returns 404.
        if opendap_url and opendap_url.endswith((".nc4", ".nc")):
            urls.append(opendap_url)
        elif opendap_url:
            log.warning("CMR: skipping non-granule URL (no .nc4/.nc suffix): %s",
                        opendap_url)

    return urls


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

    # ── Discover actual directory structure ───────────────────────────────
    # The NIES SFTP layout is not publicly documented; probe three levels from
    # root so we can find the correct data path regardless of server version.
    try:
        root_entries = sftp.listdir("/")
        _p(f"GOSAT XCH₄: SFTP root contents: {root_entries}")
        for top in root_entries:
            try:
                sub = sftp.listdir(f"/{top}")
                _p(f"GOSAT XCH₄:   /{top}/ → {sub[:10]}")
                for mid in sub[:5]:
                    try:
                        deep = sftp.listdir(f"/{top}/{mid}")
                        _p(f"GOSAT XCH₄:     /{top}/{mid}/ → {deep[:8]}")
                    except Exception as e3:
                        _p(f"GOSAT XCH₄:     /{top}/{mid}/ → (listdir error: {e3})")
            except Exception as e2:
                _p(f"GOSAT XCH₄:   /{top}/ → (listdir error: {e2})")
    except Exception as e:
        _p(f"GOSAT XCH₄: directory probe failed: {e}")

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
            remote_dir = f"/pub/gosat/SWIRFTS/NIES/L2/{year_str}/{day_str}"

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
        "pipeline_version": "1.1.0",
        "generated": datetime.now().isoformat(),
        "config": asdict(cfg),
        "output_files": results.get("files", {}),
        "stats": results.get("stats", {}),
    }
    path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    log.info("Run config → %s", path)
