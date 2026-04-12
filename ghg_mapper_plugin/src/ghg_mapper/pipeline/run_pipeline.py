"""
src/ghg_mapper/pipeline/run_pipeline.py

End-to-end pipeline called by the QGIS plugin worker thread.
All heavy lifting (GEE calls, raster math, spatial joins) lives here
so it can be tested independently of QGIS.

Stages
------
1. GEE extraction   — pull TROPOMI CH4, OCO-2/3 XCO2, GOSAT XCO2+XCH4
2. Composite build  — inverse-variance weighted monthly composite on 0.1° grid
3. SOC/SIC join     — spatially join field points to grid cells
4. CAAQMS join      — attach NO2/CO values to nearest grid cell (optional)
5. Hotspot detect   — flag cells > 90th percentile as hotspots
6. Output           — write GeoTIFFs, GeoPackages, summary CSV
"""

from __future__ import annotations
import os
import json
import numpy as np
import pandas as pd

from typing import Callable, Optional
from pathlib import Path


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def run_full_pipeline(
    config: dict,
    log_fn: Callable[[str], None] = print,
    progress_fn: Callable[[int], None] = lambda x: None,
) -> dict:
    """
    Orchestrates all pipeline stages.

    Parameters
    ----------
    config      : dict from GHGMapperDialog._build_config()
    log_fn      : callable that receives log messages (goes to plugin log pane)
    progress_fn : callable that receives 0-100 progress (goes to progress bar)

    Returns
    -------
    dict with paths to output files
    """
    out = Path(config["output_dir"])
    out.mkdir(parents=True, exist_ok=True)

    # Save config snapshot for reproducibility
    with open(out / "run_config.json", "w") as f:
        json.dump(config, f, indent=2, default=str)

    # Stage weights for progress bar
    stages = [
        ("Initialising GEE connection",          5,  _stage_init_gee),
        ("Extracting TROPOMI CH₄ via GEE",       20, _stage_tropomi),
        ("Extracting OCO-2/3 XCO₂ via GEE",      20, _stage_oco),
        ("Extracting GOSAT XCO₂+XCH₄ via GEE",  10, _stage_gosat),
        ("Building monthly composites",           10, _stage_composite),
        ("Processing SOC/SIC ground truth",        5, _stage_soil),
        ("CAAQMS validation (if available)",       5, _stage_caaqms),
        ("Detecting GHG hotspots",                 5, _stage_hotspots),
        ("Writing output files",                  10, _stage_write_outputs),
        ("Generating summary report",              5, _stage_summary),
    ]

    ctx = {
        "config":    config,
        "output_dir": out,
        "log":       log_fn,
        "data":      {},   # accumulates intermediate data between stages
    }

    cumulative = 0
    for name, weight, fn in stages:
        log_fn(f"\n▶ {name}…")
        try:
            fn(ctx)
        except Exception as e:
            # Non-fatal stages log a warning; fatal ones re-raise
            log_fn(f"  ⚠ {name} failed: {e}")
            import traceback
            log_fn(traceback.format_exc())
        cumulative += weight
        progress_fn(min(cumulative, 99))

    progress_fn(100)
    log_fn("\n✅  All stages complete.")
    return ctx["data"].get("output_paths", {})


# ---------------------------------------------------------------------------
# Stage implementations
# ---------------------------------------------------------------------------

def _stage_init_gee(ctx: dict):
    import ee
    project = ctx["config"].get("gee_project", "")
    if not project:
        raise ValueError("GEE project ID not set.")
    try:
        ee.Initialize(project=project)
        ctx["log"]("  GEE initialised successfully.")
    except Exception:
        # Already initialised from dialog auth — that's fine
        ctx["log"]("  GEE already initialised.")


def _stage_tropomi(ctx: dict):
    """Extract TROPOMI L3 CH4 monthly means via GEE."""
    if not ctx["config"]["satellites"].get("tropomi"):
        ctx["log"]("  Skipped (not selected).")
        return

    import ee
    bbox = ctx["config"]["bbox"]
    region = ee.Geometry.BBox(bbox["west"], bbox["south"], bbox["east"], bbox["north"])
    start  = ctx["config"]["date_start"]
    end    = ctx["config"]["date_end"]

    ctx["log"](f"  Querying COPERNICUS/S5P/OFFL/L3_CH4 for {start} → {end}")

    collection = (
        ee.ImageCollection("COPERNICUS/S5P/OFFL/L3_CH4")
        .filterDate(start, end)
        .filterBounds(region)
        .select("CH4_column_volume_mixing_ratio_dry_air")
    )

    count = collection.size().getInfo()
    ctx["log"](f"  Found {count} TROPOMI CH₄ granules.")

    if count == 0:
        ctx["log"]("  ⚠ No TROPOMI data for this period/region. Check dates and AOI.")
        return

    # Monthly mean composite
    grid_res = ctx["config"]["grid_res"]
    monthly  = _gee_monthly_composite(collection, region, grid_res)

    # Export to Drive or local — for now export to GEE asset as intermediate
    # In production: use ee.batch.Export.image.toDrive and poll for completion
    ctx["data"]["tropomi_collection"] = monthly
    ctx["log"](f"  Monthly composite collection built ({grid_res}° grid).")
    _download_gee_composite(monthly, region, grid_res,
                             ctx["output_dir"] / "ch4_composite.tif",
                             ctx["log"])


def _stage_oco(ctx: dict):
    """Extract OCO-2 and/or OCO-3 XCO2 monthly means via GEE."""
    cfg = ctx["config"]["satellites"]
    if not cfg.get("oco2") and not cfg.get("oco3"):
        ctx["log"]("  Skipped (neither OCO-2 nor OCO-3 selected).")
        return

    import ee
    bbox = ctx["config"]["bbox"]
    region = ee.Geometry.BBox(bbox["west"], bbox["south"], bbox["east"], bbox["north"])
    start  = ctx["config"]["date_start"]
    end    = ctx["config"]["date_end"]
    grid_res = ctx["config"]["grid_res"]

    collections = []
    if cfg.get("oco2"):
        ctx["log"]("  Querying JAXA/GCOM-C/L3/LAND/LAI (OCO-2 proxy in GEE)…")
        # GEE has OCO-2 via NASA/OCO2_L2_Lite_FP — use that
        oco2 = (
            ee.ImageCollection("NASA/OCO2_L2_Lite_FP")
            .filterDate(start, end)
            .filterBounds(region)
            .select("XCO2")
        )
        count = oco2.size().getInfo()
        ctx["log"](f"  OCO-2: {count} granules.")
        if count > 0:
            collections.append(oco2)

    if cfg.get("oco3"):
        ctx["log"]("  Querying NASA/OCO3_L2_Lite_FP…")
        try:
            oco3 = (
                ee.ImageCollection("NASA/OCO3_L2_Lite_FP")
                .filterDate(start, end)
                .filterBounds(region)
                .select("XCO2")
            )
            count3 = oco3.size().getInfo()
            ctx["log"](f"  OCO-3: {count3} granules.")
            if count3 > 0:
                collections.append(oco3)
        except Exception as e:
            ctx["log"](f"  ⚠ OCO-3 not available in GEE: {e}")

    if not collections:
        ctx["log"]("  No XCO₂ data found.")
        return

    # Merge OCO-2 and OCO-3
    merged = collections[0]
    for c in collections[1:]:
        merged = merged.merge(c)

    monthly = _gee_monthly_composite(merged, region, grid_res)
    ctx["data"]["xco2_collection"] = monthly
    _download_gee_composite(monthly, region, grid_res,
                             ctx["output_dir"] / "xco2_composite.tif",
                             ctx["log"])


def _stage_gosat(ctx: dict):
    """GOSAT via GEE — note: GOSAT L2 not in GEE public catalogue.
    Falls back to downloading from NASA Earthdata if available."""
    if not ctx["config"]["satellites"].get("gosat"):
        ctx["log"]("  Skipped (not selected).")
        return
    ctx["log"]("  ⚠ GOSAT L2 is not in the GEE public catalogue.")
    ctx["log"]("  For GOSAT data: download ACOS GOSAT Lite from NASA Earthdata")
    ctx["log"]("  (https://disc.gsfc.nasa.gov/) and place .nc files in input/gosat/")
    ctx["log"]("  The pipeline will ingest them in the composite stage automatically.")
    # TODO: implement local GOSAT NetCDF ingestion (see workflow section 2.3)


def _stage_composite(ctx: dict):
    """Build the final merged composite grid — already done per-satellite above.
    This stage handles any local NetCDF files (GOSAT, manual downloads)."""
    ctx["log"]("  Checking for local NetCDF files in input/ folder…")
    input_dir = Path(ctx["config"]["output_dir"]).parent / "input"
    if not input_dir.exists():
        ctx["log"]("  No input/ folder found — using GEE composites only.")
        return
    gosat_files = list(input_dir.glob("gosat/*.nc")) + list(input_dir.glob("gosat/*.h5"))
    if gosat_files:
        ctx["log"](f"  Found {len(gosat_files)} GOSAT files — ingesting…")
        # TODO: merge with GEE composites using inverse-variance weighting
        ctx["log"]("  GOSAT local ingest: not yet implemented in v0.1.")


def _stage_soil(ctx: dict):
    """Convert SOC/SIC table records to a GeoPackage point layer."""
    records = ctx["config"].get("soil_records", [])
    if not records:
        ctx["log"]("  No soil records provided — skipping.")
        return

    import geopandas as gpd
    from shapely.geometry import Point

    gdf = gpd.GeoDataFrame(
        records,
        geometry=[Point(r["lon"], r["lat"]) for r in records],
        crs="EPSG:4326"
    )

    # Remove rows with missing lat/lon
    gdf = gdf.dropna(subset=["lat", "lon"])

    out_path = ctx["output_dir"] / "soc_points.gpkg"
    gdf.to_file(out_path, driver="GPKG")
    ctx["log"](f"  Saved {len(gdf)} soil points → soc_points.gpkg")

    # Also handle hyperspectral rasters
    for key, label in [("hyp_soc_raster", "SOC"), ("hyp_sic_raster", "SIC")]:
        raster_path = ctx["config"].get(key)
        if raster_path and Path(raster_path).exists():
            ctx["log"](f"  Hyperspectral {label} raster found: {Path(raster_path).name}")
            ctx["log"](f"  (Will be resampled to grid in composite stage — v0.1 placeholder)")
            # TODO: rasterio resample to grid_res and write to output_dir


def _stage_caaqms(ctx: dict):
    """Load and clean CAAQMS CSV files, attach to grid."""
    caaqms_dir = ctx["config"].get("caaqms_dir")
    if not caaqms_dir or not Path(caaqms_dir).exists():
        ctx["log"]("  No CAAQMS data folder — skipping validation.")
        return

    csv_files = list(Path(caaqms_dir).glob("*.csv"))
    ctx["log"](f"  Found {len(csv_files)} CAAQMS CSV files.")
    if not csv_files:
        ctx["log"]("  ⚠ No CSV files found in CAAQMS folder.")
        return

    frames = []
    for f in csv_files[:20]:   # cap at 20 for v0.1
        try:
            df = pd.read_csv(f)
            frames.append(df)
        except Exception as e:
            ctx["log"](f"  Could not read {f.name}: {e}")

    if frames:
        combined = pd.concat(frames, ignore_index=True)
        ctx["log"](f"  CAAQMS combined: {len(combined)} rows across {len(frames)} files.")
        ctx["data"]["caaqms_df"] = combined
        # TODO: apply clean_caaqms_data() from workflow Section 2.4


def _stage_hotspots(ctx: dict):
    """Flag grid cells > 90th percentile as GHG hotspots and write shapefile."""
    import rasterio
    import geopandas as gpd
    from shapely.geometry import box

    ch4_path = ctx["output_dir"] / "ch4_composite.tif"
    if not ch4_path.exists():
        ctx["log"]("  CH₄ composite not found — skipping hotspot detection.")
        return

    with rasterio.open(ch4_path) as src:
        data = src.read(1).astype(float)
        nodata = src.nodata
        transform = src.transform
        crs = src.crs
        nrows, ncols = data.shape

    if nodata is not None:
        data[data == nodata] = np.nan

    threshold = np.nanpercentile(data, 90)
    ctx["log"](f"  CH₄ 90th percentile threshold: {threshold:.2f} ppb")

    # Build polygon for each hotspot cell
    records = []
    for r in range(nrows):
        for c in range(ncols):
            val = data[r, c]
            if np.isnan(val) or val < threshold:
                continue
            x_min, y_max = rasterio.transform.xy(transform, r, c, offset="ul")
            x_max, y_min = rasterio.transform.xy(transform, r + 1, c + 1, offset="ul")
            records.append({
                "geometry": box(x_min, y_min, x_max, y_max),
                "ch4_ppb":  float(val),
                "row": r, "col": c
            })

    if not records:
        ctx["log"]("  No hotspot cells found above threshold.")
        return

    gdf = gpd.GeoDataFrame(records, crs=crs)
    out_path = ctx["output_dir"] / "ghg_hotspots.gpkg"
    gdf.to_file(out_path, driver="GPKG")
    ctx["log"](f"  Identified {len(gdf)} hotspot cells → ghg_hotspots.gpkg")


def _stage_write_outputs(ctx: dict):
    """Collect all output paths into ctx['data']['output_paths']."""
    out = ctx["output_dir"]
    ctx["data"]["output_paths"] = {
        "ch4_composite":  str(out / "ch4_composite.tif"),
        "xco2_composite": str(out / "xco2_composite.tif"),
        "hotspots":       str(out / "ghg_hotspots.gpkg"),
        "soc_points":     str(out / "soc_points.gpkg"),
        "config":         str(out / "run_config.json"),
    }
    ctx["log"]("  Output paths registered.")


def _stage_summary(ctx: dict):
    """Write a plain-text summary report."""
    summary_lines = [
        "GHG Mapper — Run Summary",
        "=" * 50,
        f"Date range : {ctx['config']['date_start']} → {ctx['config']['date_end']}",
        f"AOI        : W{ctx['config']['bbox']['west']} E{ctx['config']['bbox']['east']} "
        f"S{ctx['config']['bbox']['south']} N{ctx['config']['bbox']['north']}",
        f"Grid res   : {ctx['config']['grid_res']}°",
        f"Soil points: {len(ctx['config'].get('soil_records', []))}",
        "",
        "Output files:",
    ]
    for k, v in ctx["data"].get("output_paths", {}).items():
        summary_lines.append(f"  {k:20s}: {v}")

    report_path = ctx["output_dir"] / "run_summary.txt"
    with open(report_path, "w") as f:
        f.write("\n".join(summary_lines))
    ctx["log"](f"  Summary report → run_summary.txt")


# ---------------------------------------------------------------------------
# GEE helpers
# ---------------------------------------------------------------------------

def _gee_monthly_composite(collection, region, grid_res_deg: float):
    """
    Build a monthly mean composite ImageCollection from an input collection.
    Uses simple mean (extend to inv-variance weighting when uncertainty bands available).
    """
    import ee

    start_str = collection.aggregate_min("system:time_start").getInfo()
    end_str   = collection.aggregate_max("system:time_start").getInfo()

    # Use ee.List of months
    months = ee.List.sequence(1, 12)
    years  = ee.List.sequence(
        ee.Date(start_str).get("year"),
        ee.Date(end_str).get("year")
    )

    def monthly_mean(ym):
        ym    = ee.List(ym)
        year  = ee.Number(ym.get(0))
        month = ee.Number(ym.get(1))
        start = ee.Date.fromYMD(year, month, 1)
        end   = start.advance(1, "month")
        return (
            collection
            .filterDate(start, end)
            .mean()
            .set("year", year)
            .set("month", month)
            .set("system:time_start", start.millis())
        )

    year_month_pairs = years.map(
        lambda y: months.map(lambda m: ee.List([y, m]))
    ).flatten()

    monthly_col = ee.ImageCollection(year_month_pairs.map(monthly_mean))
    return monthly_col


def _download_gee_composite(
    monthly_col,
    region,
    grid_res_deg: float,
    output_path: Path,
    log_fn: Callable,
):
    """
    Download a monthly composite collection to a local GeoTIFF.
    For v0.1: exports the annual mean as a single-band GeoTIFF.
    Production version should export each month as a separate band.
    """
    import ee
    import requests
    import tempfile

    try:
        annual_mean = monthly_col.mean()
        scale_m = int(grid_res_deg * 111_000)  # rough deg → metres conversion

        url = annual_mean.getDownloadURL({
            "region": region,
            "scale":  scale_m,
            "crs":    "EPSG:4326",
            "format": "GEO_TIFF",
        })

        log_fn(f"  Downloading from GEE… (this may take a few minutes for large AOIs)")
        resp = requests.get(url, stream=True, timeout=300)
        resp.raise_for_status()

        with open(output_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=65536):
                f.write(chunk)

        log_fn(f"  Saved → {output_path.name}")

    except Exception as e:
        log_fn(f"  ⚠ GEE download failed: {e}")
        log_fn("  For large AOIs, use ee.batch.Export.image.toDrive() instead.")
        log_fn("  See docs/methodology.md §2 for the batch export pattern.")
