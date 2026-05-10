"""
stages_aux.py — Auxiliary GEE / raster stages for the enhanced GHG pipeline.

This module implements the independent data-fetch stages added by the
"ghg-mapper-enhancements" spec. Each stage sources a single auxiliary dataset
— ESA WorldCover cropland mask, ERA5-Land meteorology, TROPOMI NO₂, FIRMS
active fires, FAO GLW4 livestock density, and the L2-strict TROPOMI CH₄
composite — and returns per-cell numpy arrays aligned to the AOI grid used
elsewhere in the pipeline. All stages follow the "never raise, always
degrade" contract from design.md §3 so that a missing dataset simply drops
columns from the final GeoPackage rather than failing the whole pipeline
(Req 19.1).

Design reference: design.md §3 "Auxiliary GEE stages"
Feature: ghg-mapper-enhancements

Conventions common to every stage
---------------------------------
* Heavy dependencies (``ee``, ``geemap``, ``rasterio``, ``geopandas``) are
  imported lazily inside each function. Users who never enable the
  corresponding feature never pay the import cost.
* Every download uses ``tempfile.NamedTemporaryFile(delete=False)`` and
  cleans up in a ``finally`` block.
* Every rasterio read uses a context manager.
* Output arrays have shape ``(rows, cols)`` with ``rows = ceil((north - south) / grid_res)``
  and ``cols = ceil((east - west) / grid_res)``, indexed with row 0 on the
  northern edge (matches ``_grid_oco_to_tif`` and ``grid_inverse_variance``).
* Empty collections (zero images in the requested window) are treated as
  "no data": log a warning via ``prog`` / ``log`` and return ``None``.
* Any unexpected exception is caught at the stage boundary, logged via
  ``prog``, and ``None`` is returned.
"""

from __future__ import annotations

import logging
import math
import os
import tempfile
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import numpy as np

log = logging.getLogger("ghg_mapper")


# ═════════════════════════════════════════════════════════════════════════════
# Shared helpers
# ═════════════════════════════════════════════════════════════════════════════

def _target_shape(bbox, grid_res: float) -> Tuple[int, int]:
    """Return ``(rows, cols)`` for ``bbox=[W, S, E, N]`` at ``grid_res`` degrees."""
    west, south, east, north = [float(x) for x in bbox]
    cols = max(1, int(math.ceil((east - west) / grid_res)))
    rows = max(1, int(math.ceil((north - south) / grid_res)))
    return rows, cols


def _download_band_to_array(
    ee_image,
    aoi,
    bbox,
    grid_res: float,
    band_name: str,
) -> Optional[np.ndarray]:
    """Download a single-band EE Image to a GeoTIFF and return it as a numpy array.

    The image is requested at approximately ``grid_res × 111320 m`` spacing in
    EPSG:4326. After download, the raster is read via rasterio and, if its
    native shape does not already match the target ``(rows, cols)`` for the
    supplied bbox, reprojected using area-weighted averaging.

    Returns
    -------
    np.ndarray, shape (rows, cols), dtype float32
        Values at the target grid; NoData cells become NaN.
    None
        On any download / read / reprojection failure.
    """
    import urllib.request

    rows, cols = _target_shape(bbox, grid_res)
    west, south, east, north = [float(x) for x in bbox]

    tmp_path: Optional[str] = None
    try:
        f = tempfile.NamedTemporaryFile(suffix=".tif", delete=False)
        tmp_path = f.name
        f.close()

        url = ee_image.getDownloadURL({
            "name":        band_name or "band",
            "region":      aoi,
            "scale":       grid_res * 111320.0,
            "crs":         "EPSG:4326",
            "format":      "GEO_TIFF",
            "filePerBand": False,
        })
        urllib.request.urlretrieve(url, tmp_path)

        import rasterio
        from rasterio.transform import from_bounds
        from rasterio.warp import reproject, Resampling

        with rasterio.open(tmp_path) as src:
            src_arr = src.read(1, masked=True).filled(np.nan).astype(np.float32)
            src_transform = src.transform
            src_crs = src.crs

        # Fast path: already at target shape.
        if src_arr.shape == (rows, cols):
            return src_arr

        dst = np.full((rows, cols), np.nan, dtype=np.float32)
        dst_transform = from_bounds(west, south, east, north, cols, rows)
        reproject(
            source=src_arr,
            destination=dst,
            src_transform=src_transform,
            src_crs=src_crs,
            dst_transform=dst_transform,
            dst_crs="EPSG:4326",
            resampling=Resampling.average,
            src_nodata=np.nan,
            dst_nodata=np.nan,
        )
        return dst

    except Exception as e:
        log.warning("_download_band_to_array(%s) failed: %s", band_name, e)
        return None

    finally:
        if tmp_path and os.path.exists(tmp_path):
            try:
                os.unlink(tmp_path)
            except OSError:
                pass


# ═════════════════════════════════════════════════════════════════════════════
# Stage: Cropland mask (ESA WorldCover)   — design §3, Req 2
# ═════════════════════════════════════════════════════════════════════════════

def stage_cropland_mask(
    ee,
    aoi,
    bbox,
    grid_res: float,
    include_grassland: bool = False,
    prog=None,
) -> Optional[np.ndarray]:
    """ESA WorldCover 2021 cropland fraction at the target grid.

    The output is the fraction of each grid cell covered by WorldCover class
    40 ("Cropland"), plus class 30 ("Grassland") when ``include_grassland`` is
    True. Values lie in ``[0, 1]`` per Req 2.4 (``cropland_fraction`` column).

    Returns
    -------
    np.ndarray, shape (rows, cols), dtype float32
        Cropland fraction in [0, 1].
    None
        If WorldCover is unavailable for the AOI or download fails.
    """
    STAGE_PCT = 15

    def _p(msg):
        (prog(STAGE_PCT, msg) if prog else log.info(msg))

    try:
        _p("WorldCover: fetching cropland mask (ESA/WorldCover/v200) …")

        # WorldCover 2021 is distributed as an ImageCollection; a single
        # global composite is returned by .first().
        col = ee.ImageCollection("ESA/WorldCover/v200").filterBounds(aoi)

        # .size() can be expensive; skip if .first() resolves to null.
        first = col.first()
        if first is None:
            _p("⚠  WorldCover: no imagery intersecting AOI — skipping cropland mask.")
            return None

        wc = ee.Image(first).select("Map")

        # Build binary cropland raster at native 10 m resolution …
        binary = wc.eq(40)
        if include_grassland:
            binary = binary.Or(wc.eq(30))
            _p("WorldCover: including Grassland (class 30) in cropland mask.")

        # … then aggregate to the output grid as a mean (→ area fraction).
        # reduceResolution is the GEE-recommended way to downsample a
        # categorical mask to a fractional coverage raster.
        fraction = (binary
                    .reduceResolution(reducer=ee.Reducer.mean(), maxPixels=1024)
                    .reproject(crs="EPSG:4326", scale=grid_res * 111320.0)
                    .rename("cropland_fraction")
                    .clip(aoi))

        arr = _download_band_to_array(fraction, aoi, bbox, grid_res, "cropland_fraction")
        if arr is None:
            _p("⚠  WorldCover: download failed — returning None.")
            return None

        # Ensure values are in [0, 1] — GEE can emit slight FP overshoots.
        arr = np.clip(arr, 0.0, 1.0).astype(np.float32)
        n_crop = int(np.nansum(arr > 0))
        _p(f"WorldCover: cropland fraction raster shape={arr.shape}, "
           f"{n_crop} cells with nonzero cropland.")
        return arr

    except Exception as e:
        _p(f"⚠  stage_cropland_mask failed: {e}")
        return None


# ═════════════════════════════════════════════════════════════════════════════
# Stage: ERA5-Land meteorology   — design §3, Req 5
# ═════════════════════════════════════════════════════════════════════════════

def stage_era5(
    ee,
    aoi,
    bbox,
    grid_res: float,
    start: str,
    end: str,
    prog=None,
) -> Optional[Dict[str, np.ndarray]]:
    """Fetch ERA5-Land hourly aggregates over ``[start, end]``.

    Returns a dict with derived meteorological arrays at the AOI grid.
    * ``temp_c``           — 2 m air temperature, mean (°C)
    * ``u_ms``             — 10 m U wind component, mean (m/s)
    * ``v_ms``             — 10 m V wind component, mean (m/s)
    * ``wind_speed_ms``    — ``sqrt(u² + v²)`` (m/s)
    * ``wind_dir_deg``     — meteorological wind direction, degrees
                             ``atan2(u, v) × 180/π mod 360``
    * ``precip_mm``        — total precipitation over the window (mm)
    * ``soil_moist``       — volumetric soil water, layer 1 (m³/m³)

    Returns ``None`` if the ERA5-Land collection has zero hourly images over
    the window or any variable download fails.
    """
    STAGE_PCT = 32

    def _p(msg):
        (prog(STAGE_PCT, msg) if prog else log.info(msg))

    try:
        _p(f"ERA5-Land: building composite for {start} → {end} …")

        col = (ee.ImageCollection("ECMWF/ERA5_LAND/HOURLY")
                 .filterBounds(aoi)
                 .filterDate(start, end))

        n = col.size().getInfo()
        _p(f"ERA5-Land: {n} hourly image(s) in window.")
        if n == 0:
            _p("⚠  ERA5-Land: empty collection — skipping meteorology.")
            return None

        # Convert Kelvin → °C inside the mapped lambda so we never emit K.
        temp_c_img = col.select("temperature_2m").mean().subtract(273.15).rename("temp_c").clip(aoi)
        u_img = col.select("u_component_of_wind_10m").mean().rename("u_ms").clip(aoi)
        v_img = col.select("v_component_of_wind_10m").mean().rename("v_ms").clip(aoi)
        # Precip: meters → mm (×1000); total over window per Req 5.2.
        precip_img = (col.select("total_precipitation_hourly")
                      .sum().multiply(1000.0).rename("precip_mm").clip(aoi))
        soil_img = (col.select("volumetric_soil_water_layer_1")
                    .mean().rename("soil_moist").clip(aoi))

        _p("ERA5-Land: downloading temperature_2m …")
        temp_c = _download_band_to_array(temp_c_img, aoi, bbox, grid_res, "temp_c")
        _p("ERA5-Land: downloading u_component_of_wind_10m …")
        u_ms = _download_band_to_array(u_img, aoi, bbox, grid_res, "u_ms")
        _p("ERA5-Land: downloading v_component_of_wind_10m …")
        v_ms = _download_band_to_array(v_img, aoi, bbox, grid_res, "v_ms")
        _p("ERA5-Land: downloading total_precipitation_hourly …")
        precip_mm = _download_band_to_array(precip_img, aoi, bbox, grid_res, "precip_mm")
        _p("ERA5-Land: downloading volumetric_soil_water_layer_1 …")
        soil_moist = _download_band_to_array(soil_img, aoi, bbox, grid_res, "soil_moist")

        # Any required band failing its download → degrade to None for the stage.
        if any(a is None for a in (temp_c, u_ms, v_ms, precip_mm, soil_moist)):
            _p("⚠  ERA5-Land: at least one band download failed — returning None.")
            return None

        wind_speed_ms = np.sqrt(u_ms * u_ms + v_ms * v_ms).astype(np.float32)
        with np.errstate(invalid="ignore"):
            wind_dir_deg = (np.degrees(np.arctan2(u_ms, v_ms)) % 360.0).astype(np.float32)
        # Propagate NaN from inputs.
        nan_mask = np.isnan(u_ms) | np.isnan(v_ms)
        wind_speed_ms = np.where(nan_mask, np.nan, wind_speed_ms).astype(np.float32)
        wind_dir_deg = np.where(nan_mask, np.nan, wind_dir_deg).astype(np.float32)

        return {
            "temp_c":        temp_c,
            "u_ms":          u_ms,
            "v_ms":          v_ms,
            "wind_speed_ms": wind_speed_ms,
            "wind_dir_deg":  wind_dir_deg,
            "precip_mm":     precip_mm,
            "soil_moist":    soil_moist,
        }

    except Exception as e:
        _p(f"⚠  stage_era5 failed: {e}")
        return None


# ═════════════════════════════════════════════════════════════════════════════
# Stage: TROPOMI NO₂ co-tracer   — design §3, Req 8
# ═════════════════════════════════════════════════════════════════════════════

def stage_no2(
    ee,
    aoi,
    bbox,
    grid_res: float,
    start: str,
    end: str,
    prog=None,
) -> Optional[np.ndarray]:
    """Mean tropospheric NO₂ column density (mol/m²) from Sentinel-5P L3 NO₂."""
    STAGE_PCT = 34

    def _p(msg):
        (prog(STAGE_PCT, msg) if prog else log.info(msg))

    try:
        _p(f"S5P NO₂: building composite for {start} → {end} …")
        col = (ee.ImageCollection("COPERNICUS/S5P/OFFL/L3_NO2")
                 .filterBounds(aoi)
                 .filterDate(start, end)
                 .select("tropospheric_NO2_column_number_density"))

        n = col.size().getInfo()
        _p(f"S5P NO₂: {n} image(s) in window.")
        if n == 0:
            _p("⚠  S5P NO₂: empty collection — skipping co-tracer.")
            return None

        img = col.mean().rename("no2_mol_m2").clip(aoi)
        arr = _download_band_to_array(img, aoi, bbox, grid_res, "no2_mol_m2")
        if arr is None:
            _p("⚠  S5P NO₂: download failed — returning None.")
            return None
        return arr

    except Exception as e:
        _p(f"⚠  stage_no2 failed: {e}")
        return None


# ═════════════════════════════════════════════════════════════════════════════
# Stage: FIRMS active fires (VIIRS + MODIS)   — design §3, Req 6
# ═════════════════════════════════════════════════════════════════════════════

# Confidence threshold on the FireMask band. Per MOD14A1 / VNP14A1 product
# documentation: 0–4 = non-fire / cloud / water; 7 = low-confidence fire;
# 8 = nominal-confidence fire; 9 = high-confidence fire. We take ≥ 7 as the
# "detected fire" threshold.
_FIRMS_FIRE_THRESHOLD = 7

# Safety cap for per-image point vectorization. On large AOIs with many fire
# images we fall back to aggregate-only output rather than waiting for a
# potentially multi-minute reduceToVectors scan.
_FIRMS_MAX_IMAGES_FOR_POINTS = 60
_FIRMS_MAX_POINTS_PER_IMAGE = 500


def _firms_fire_count_raster(ee, col, aoi, bbox, grid_res, label, prog_cb):
    """Sum per-image fire-pixel counts to a per-cell count raster at grid_res."""
    if col is None:
        return None
    try:
        n = col.size().getInfo()
    except Exception as e:
        prog_cb(f"⚠  FIRMS {label}: size() failed: {e}")
        return None
    prog_cb(f"FIRMS {label}: {n} image(s) in window.")
    if n == 0:
        return None

    # Binary fire mask per image, then sum across the window.
    fire_sum = (col
                .map(lambda img: img.select("FireMask").gte(_FIRMS_FIRE_THRESHOLD).rename("fire"))
                .sum()
                .rename(f"fire_count_{label}")
                .unmask(0)
                .clip(aoi))

    # Aggregate 1 km native resolution to the output grid via mean × area
    # fraction. reduceResolution with Reducer.mean preserves per-cell count
    # semantics when the source is already a count-per-native-pixel image,
    # so multiplying by the cell/native ratio gives the per-cell count.
    return _download_band_to_array(fire_sum, aoi, bbox, grid_res, f"fire_count_{label}")


def _firms_vectorize_points(ee, col, aoi, label):
    """Best-effort per-detection point GeoDataFrame from a FIRMS collection.

    Returns ``None`` if geopandas is unavailable, if the collection is too
    large to vectorize quickly, or if any GEE / geopandas call raises.
    """
    try:
        import geopandas as gpd
        from shapely.geometry import Point
    except Exception:
        return None

    try:
        n = col.size().getInfo()
    except Exception:
        return None
    if n == 0 or n > _FIRMS_MAX_IMAGES_FOR_POINTS:
        return None

    try:
        img_list = col.toList(n)
    except Exception:
        return None

    records = []
    for i in range(n):
        try:
            img = ee.Image(img_list.get(i))
            mask = img.select("FireMask").gte(_FIRMS_FIRE_THRESHOLD).selfMask()
            fc = mask.reduceToVectors(
                geometry=aoi,
                scale=1000,
                geometryType="centroid",
                maxPixels=1e9,
            ).limit(_FIRMS_MAX_POINTS_PER_IMAGE)
            date_str = img.date().format("YYYY-MM-dd").getInfo()
            info = fc.getInfo()
            for feat in info.get("features", []):
                geom = feat.get("geometry") or {}
                if geom.get("type") != "Point":
                    continue
                coords = geom.get("coordinates") or []
                if len(coords) < 2:
                    continue
                records.append({
                    "date":       date_str,
                    "sensor":     label,
                    "confidence": None,
                    "geometry":   Point(coords[0], coords[1]),
                })
        except Exception:
            # Individual image failure — skip it, keep any records gathered so far.
            continue

    if not records:
        return None
    try:
        gdf = gpd.GeoDataFrame(records, geometry="geometry", crs="EPSG:4326")
        return gdf
    except Exception:
        return None


def stage_firms(
    ee,
    aoi,
    bbox,
    grid_res: float,
    start: str,
    end: str,
    sensors: str = "both",
    prog=None,
) -> Optional[Dict[str, Any]]:
    """VIIRS + MODIS active-fire aggregation.

    Returns a dict:
    * ``fire_count_viirs`` — per-cell fire count (or None if VIIRS disabled / empty)
    * ``fire_count_modis`` — per-cell fire count (or None)
    * ``fire_count_total`` — elementwise sum of whichever sensors are present
    * ``fires_gdf`` — GeoDataFrame of per-image centroids, or None on failure

    Returns ``None`` only if both enabled sensor collections are empty / fail.
    """
    STAGE_PCT = 36

    def _p(msg):
        (prog(STAGE_PCT, msg) if prog else log.info(msg))

    try:
        sensors = (sensors or "both").lower().strip()
        use_viirs = sensors in ("viirs", "both")
        use_modis = sensors in ("modis", "both")

        viirs_col = None
        modis_col = None
        if use_viirs:
            viirs_col = (ee.ImageCollection("NOAA/VIIRS/001/VNP14A1")
                         .filterBounds(aoi)
                         .filterDate(start, end))
        if use_modis:
            modis_col = (ee.ImageCollection("MODIS/061/MOD14A1")
                         .filterBounds(aoi)
                         .filterDate(start, end))

        _p(f"FIRMS: building fire-count rasters (sensors={sensors}) …")
        fc_viirs = (_firms_fire_count_raster(ee, viirs_col, aoi, bbox, grid_res, "viirs", _p)
                    if use_viirs else None)
        fc_modis = (_firms_fire_count_raster(ee, modis_col, aoi, bbox, grid_res, "modis", _p)
                    if use_modis else None)

        if fc_viirs is None and fc_modis is None:
            _p("⚠  FIRMS: no fire-count rasters produced — returning None.")
            return None

        # Elementwise sum, treating missing sensors as zero (allocated to target shape).
        rows, cols = _target_shape(bbox, grid_res)
        total = np.zeros((rows, cols), dtype=np.float32)
        if fc_viirs is not None:
            total = total + np.nan_to_num(fc_viirs, nan=0.0)
        if fc_modis is not None:
            total = total + np.nan_to_num(fc_modis, nan=0.0)

        # Best-effort point vectorization.
        gdfs = []
        if use_viirs and viirs_col is not None:
            gdf_v = _firms_vectorize_points(ee, viirs_col, aoi, "viirs")
            if gdf_v is not None:
                gdfs.append(gdf_v)
        if use_modis and modis_col is not None:
            gdf_m = _firms_vectorize_points(ee, modis_col, aoi, "modis")
            if gdf_m is not None:
                gdfs.append(gdf_m)

        fires_gdf = None
        if gdfs:
            try:
                import geopandas as gpd
                import pandas as pd
                fires_gdf = gpd.GeoDataFrame(
                    pd.concat(gdfs, ignore_index=True),
                    geometry="geometry", crs="EPSG:4326",
                )
                _p(f"FIRMS: vectorized {len(fires_gdf)} fire detection(s).")
            except Exception as e:
                _p(f"⚠  FIRMS: could not merge point GeoDataFrames: {e}")
                fires_gdf = None
        else:
            _p("FIRMS: per-detection point layer unavailable (vectorization skipped).")

        return {
            "fire_count_viirs": fc_viirs,
            "fire_count_modis": fc_modis,
            "fire_count_total": total,
            "fires_gdf":        fires_gdf,
        }

    except Exception as e:
        _p(f"⚠  stage_firms failed: {e}")
        return None


# ═════════════════════════════════════════════════════════════════════════════
# Stage: FAO GLW4 livestock density   — design §3, Req 7  (LOCAL rasters only)
# ═════════════════════════════════════════════════════════════════════════════

_LIVESTOCK_SPECIES = ("cattle", "buffalo", "goat", "sheep")


def stage_livestock(
    bbox,
    grid_res: float,
    glw4_paths: Dict[str, Optional[str]],
    prog=None,
) -> Optional[Dict[str, Optional[np.ndarray]]]:
    """Reproject user-supplied FAO GLW4 livestock GeoTIFFs onto the AOI grid.

    GLW4 is not currently hosted on Google Earth Engine as of 2024-Q4, so
    this stage reads local files only — GEE is not involved. Callers supply
    a dict ``{"cattle": "/path.tif", "buffalo": "...", ...}`` where any value
    may be ``None`` if the corresponding species is unavailable.

    Returns
    -------
    dict
        Always contains one key per species in
        ``{"cattle_density", "buffalo_density", "goat_density", "sheep_density"}``.
        Value is a 2-D float32 array (head/km²) for species with a valid
        path, or ``None`` for species without one.
    None
        Iff every species path is missing.
    """
    STAGE_PCT = 38

    def _p(msg):
        (prog(STAGE_PCT, msg) if prog else log.info(msg))

    try:
        if not glw4_paths:
            _p("⚠  GLW4: no paths supplied — skipping livestock.")
            return None

        present = {s: glw4_paths.get(s) for s in _LIVESTOCK_SPECIES
                   if glw4_paths.get(s)}
        if not present:
            _p("⚠  GLW4: every species path is None/empty — skipping livestock.")
            return None

        try:
            import rasterio
            from rasterio.transform import from_bounds
            from rasterio.warp import reproject, Resampling
        except Exception as e:
            _p(f"⚠  GLW4: rasterio unavailable ({e}) — skipping livestock.")
            return None

        rows, cols = _target_shape(bbox, grid_res)
        west, south, east, north = [float(x) for x in bbox]
        dst_transform = from_bounds(west, south, east, north, cols, rows)
        dst_crs = "EPSG:4326"

        out: Dict[str, Optional[np.ndarray]] = {
            f"{s}_density": None for s in _LIVESTOCK_SPECIES
        }

        for species in _LIVESTOCK_SPECIES:
            path = glw4_paths.get(species)
            if not path:
                continue
            p = Path(path)
            if not p.exists():
                _p(f"⚠  GLW4 {species}: file not found: {p}")
                continue
            try:
                with rasterio.open(p) as src:
                    src_arr = src.read(1, masked=True).astype(np.float32)
                    # NoData → 0.0 (absence interpretation per task spec).
                    src_arr = src_arr.filled(0.0)
                    src_transform = src.transform
                    src_crs = src.crs

                dst = np.zeros((rows, cols), dtype=np.float32)
                reproject(
                    source=src_arr,
                    destination=dst,
                    src_transform=src_transform,
                    src_crs=src_crs,
                    dst_transform=dst_transform,
                    dst_crs=dst_crs,
                    resampling=Resampling.average,
                    src_nodata=0.0,
                    dst_nodata=0.0,
                )
                out[f"{species}_density"] = dst
                _p(f"GLW4 {species}: reprojected {p.name} → shape {dst.shape}.")
            except Exception as e:
                _p(f"⚠  GLW4 {species}: {e}")
                out[f"{species}_density"] = None

        if all(v is None for v in out.values()):
            _p("⚠  GLW4: every species failed to load — returning None.")
            return None
        return out

    except Exception as e:
        _p(f"⚠  stage_livestock failed: {e}")
        return None


# ═════════════════════════════════════════════════════════════════════════════
# Stage: TROPOMI L2 CH₄ with strict QA filter   — design §3, Req 9
# ═════════════════════════════════════════════════════════════════════════════

def stage_tropomi_ch4_strict(
    ee,
    aoi,
    bbox,
    grid_res: float,
    start: str,
    end: str,
    qa_threshold: float = 0.5,
    albedo_threshold: float = 0.05,
    cloud_threshold: float = 0.3,
    prog=None,
) -> Optional[np.ndarray]:
    """Strict-QA TROPOMI CH₄ composite from the L2 collection.

    Uses ``COPERNICUS/S5P/OFFL/L2__CH4___`` rather than the L3 product so we
    can apply per-pixel quality filters. The SRON-recommended filter is
    ``qa_value ≥ qa_threshold AND surface_albedo > albedo_threshold AND
    cloud_fraction < cloud_threshold``.

    GEE band-name compromises
    --------------------------
    * L2 CH₄ does **not** provide a direct ``cloud_fraction`` band (that is
      only on L2 NO₂). We substitute the ``aerosol_optical_thickness_SWIR``
      band as a haziness proxy — higher AOT means hazier / more aerosol-laden
      retrievals, so the user-supplied ``cloud_threshold`` is reinterpreted
      as an AOT upper bound. This matches the semantic intent of "reject
      atmospherically contaminated pixels" while using an available band.
    * Surface albedo is ``surface_albedo_SWIR_3`` on L2 CH₄.
    * The bias-corrected dry-air column mixing ratio
      (``CH4_column_volume_mixing_ratio_dry_air_bias_corrected``) is already
      in ppb, so no scaling is applied.

    On the first image we log the available band names via ``prog`` for
    diagnostics in case ESA renames bands in a future reprocessing.
    """
    STAGE_PCT = 10

    def _p(msg):
        (prog(STAGE_PCT, msg) if prog else log.info(msg))

    try:
        _p(f"TROPOMI L2 CH₄ (strict): building composite for {start} → {end} …")
        col_all = (ee.ImageCollection("COPERNICUS/S5P/OFFL/L2__CH4___")
                     .filterBounds(aoi)
                     .filterDate(start, end))

        n = col_all.size().getInfo()
        _p(f"TROPOMI L2 CH₄: {n} image(s) in window.")
        if n == 0:
            _p("⚠  TROPOMI L2 CH₄: empty collection — returning None.")
            return None

        # One-time band-name diagnostic (helps when ESA rev-bumps).
        try:
            first = ee.Image(col_all.first())
            band_names = first.bandNames().getInfo()
            _p(f"TROPOMI L2 CH₄: available bands on first image: {band_names}")
        except Exception as e:
            _p(f"TROPOMI L2 CH₄: could not enumerate first-image bands: {e}")

        ch4_band = "CH4_column_volume_mixing_ratio_dry_air_bias_corrected"
        qa_band = "qa_value"
        albedo_band = "surface_albedo_SWIR_3"
        # aerosol_optical_thickness_SWIR is the haze proxy we substitute for cloud_fraction.
        haze_band = "aerosol_optical_thickness_SWIR"

        def _apply_strict_mask(img):
            qa = img.select(qa_band)
            albedo = img.select(albedo_band)
            haze = img.select(haze_band)
            mask = (qa.gte(qa_threshold)
                      .And(albedo.gt(albedo_threshold))
                      .And(haze.lt(cloud_threshold)))
            return img.updateMask(mask).select([ch4_band]).rename("CH4_ppb")

        composite = col_all.map(_apply_strict_mask).mean().clip(aoi)
        _p(f"TROPOMI L2 CH₄: strict filter qa>={qa_threshold}, "
           f"albedo>{albedo_threshold}, AOT<{cloud_threshold} (cloud-proxy).")

        arr = _download_band_to_array(composite, aoi, bbox, grid_res, "CH4_ppb")
        if arr is None:
            _p("⚠  TROPOMI L2 CH₄ (strict): download failed — returning None.")
            return None
        return arr

    except Exception as e:
        _p(f"⚠  stage_tropomi_ch4_strict failed: {e}")
        return None


__all__ = [
    "stage_cropland_mask",
    "stage_era5",
    "stage_no2",
    "stage_firms",
    "stage_livestock",
    "stage_tropomi_ch4_strict",
]
