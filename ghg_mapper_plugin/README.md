# GHG Mapper — Agricultural India

**Multi-satellite GHG hotspot mapping with SOC/SIC ground-truth integration.**  
A QGIS plugin + Python backend for no-code satellite emission analysis.

---

## What it does

GHG Mapper merges retrievals from TROPOMI (CH₄), OCO-2/OCO-3 (XCO₂), and GOSAT
(XCO₂ + XCH₄) into monthly composite hotspot maps over agricultural India.
It integrates your field-measured SOC and SIC values through a point-and-click
dialog — no Python coding required.

Built on the workflow described in:  
> Bander, T. (2024). *Multi-Satellite GHG Emission Hotspot Mapping over Agricultural India.*  
> Amity University, Noida.

---

## Quick Start

### 1. Install backend dependencies

```bash
conda env create -f environment.yml
conda activate ghg-mapper
```

### 2. Install the QGIS plugin

Copy the `plugins/ghg_mapper/` folder to your QGIS plugins directory:

- **Windows**: `%APPDATA%\QGIS\QGIS3\profiles\default\python\plugins\`
- **Linux/Mac**: `~/.local/share/QGIS/QGIS3/profiles/default/python/plugins/`

Then in QGIS: *Plugins → Manage and Install Plugins → Installed → ✅ GHG Mapper*

### 3. Authenticate with Google Earth Engine

In QGIS, open the plugin (`Raster → GHG Mapper → Open GHG Mapper`),
go to the **Setup** tab, enter your GEE project ID, and click **Authenticate**.

Free GEE account: https://earthengine.google.com/signup/

### 4. Enter your SOC/SIC values

On the **Ground Truth** tab, type or import your field sample values.
SOC from Walkley-Black? Tick the correction factor checkbox — it applies ×1.334 automatically.

### 5. Run

Click **▶ Run Pipeline** on the Run tab. Outputs appear in your chosen folder.

---

## Methodology options (v0.3)

The v0.3 release adds 17 opt-in methodology upgrades grouped under a new
**Methodology** tab in the dialog. Every option defaults to "off" (or to the
pre-0.3 behavior). A vanilla run with the Methodology tab untouched produces
output byte-identical to v0.1 / v0.2, so existing workflows keep working.

### Credential matrix

| Feature group                     | GEE | EarthData | User-supplied files |
|-----------------------------------|-----|-----------|---------------------|
| Enhancement / masking / compositing / inv-variance / priority | ✓ | ✓ (for OCO/GOSAT XCO₂) | — |
| ERA5, FIRMS, NO₂, WorldCover, strict TROPOMI L2 | ✓ | — | — |
| Livestock density (FAO GLW4)      | —   | —         | GLW4 GeoTIFFs (`cattle`, `buffalo`, `goat`, `sheep`) |
| CAAQMS bias calibration           | —   | —         | CPCB CAAQMS CSV (already in v0.2) |

### Hotspot classification

**Use enhancement above background** (`enhancement_mode`)
Switches the hotspot rule from "concentration ≥ 90th-percentile of AOI" to
"enhancement ≥ threshold", where enhancement is concentration minus a local
percentile background. Enable whenever regional gradients (e.g. IGP haze,
seasonal XCO₂ cycle) risk biasing the classifier. Exposes a window size,
background percentile, and per-species thresholds in ppb/ppm.

### Spatial masking

**Restrict to cropland (ESA WorldCover)** (`cropland_mask`)
Multiplies every composite by a WorldCover-derived boolean cropland mask so
urban, water, and forest cells are excluded from hotspot detection. Tick
**Include grassland** (`include_grassland`) to also admit WorldCover class 30.
Enable whenever the analysis is narrowly agricultural.

### Temporal compositing

**Compositing mode** (`compositing_mode`)
Selects how the date range is partitioned before compositing. `whole_period`
is the legacy default. `monthly` produces one composite set per calendar
month. `seasonal_in` produces Kharif / Rabi / Zaid segments aligned with
Indian agricultural seasons. `custom` exposes a table for user-defined
named windows. Non-`whole_period` modes write outputs into per-window
subfolders (e.g. `2023-07_monthly/`, `kharif_2023/`).

### Data fusion

**ERA5 meteorological co-drivers** (`use_era5`)
Fetches ERA5-Land hourly temperature, 10 m u/v wind, total precipitation,
and soil moisture averaged over each window. Enable for upwind source
attribution and mass-balance flux. Required by the flux and
upwind-centroid columns.

**FIRMS active fires** (`use_firms`, `firms_sensors`)
Aggregates VIIRS and/or MODIS active-fire detections per cell and writes
`fires.gpkg`. Enable when investigating episodic CH₄/CO₂ spikes from stubble
burning. The sensor selector picks VIIRS, MODIS, or both.

**TROPOMI NO₂ co-tracer** (`use_no2_cotracer`, `no2_high_percentile`,
`no2_low_percentile`)
Fetches a S5P NO₂ composite and tags each CH₄ hotspot as `combustion`
(high NO₂), `biogenic` (low NO₂), or `ambiguous`. Enable to separate
urban/industrial plumes from paddy/livestock signals.

**Livestock density (FAO GLW4)** (`use_livestock`,
`glw4_{cattle,buffalo,goat,sheep}_path`)
Reads user-supplied GLW4 GeoTIFFs, resamples to the grid resolution, and
adds a bottom-up IPCC Tier 1 enteric CH₄ baseline column. Enable to
cross-check satellite CH₄ hotspots against animal density. The dialog
links to the FAO GLW4 download portal.

### Quality & uncertainty

**Strict TROPOMI QA** (`strict_tropomi_qa`, `tropomi_qa_threshold`,
`tropomi_albedo_threshold`, `tropomi_cloud_threshold`)
Switches from the L3 gridded product to per-pixel L2 (`L2__CH4___`) and
applies the SRON-recommended `qa_value ≥ 0.5 AND surface_albedo > 0.05
AND cloud_fraction < 0.3` filter. Enable when hazy IGP conditions would
otherwise degrade the L3 composite.

**Inverse-variance XCO₂ gridding** (`inverse_variance_weighting`)
Weights each OCO/GOSAT sounding by `1 / σ²` using the reported
`xco2_uncertainty`, and writes a companion `xco2_composite_stderr.tif`.
Default-on in v0.3 — this is a strict improvement over the arithmetic mean
when retrieval uncertainties vary (e.g. target-mode vs nadir soundings).

**Minimum retrievals per cell** (`min_retrievals_per_cell`)
Masks cells with fewer than N valid retrievals as "insufficient data" in
both the raster and the hotspot GeoPackage. Default 5. Raise for robust
publication-grade maps; lower to 1 to reproduce legacy behavior.

**CAAQMS bias calibration** (`caaqms_bias_correction`, `idw_power`)
Computes the mean `station - satellite` bias at each CAAQMS station,
interpolates it across the AOI via IDW with the given power, and writes
`*_bias_corrected.tif` alongside the uncorrected composites. Disabled
unless a CAAQMS CSV is loaded on the existing Ground Truth tab.

### Scoring & scale

**SOC × emission priority score** (`compute_priority`)
Combines a normalized emission signal (flux > enhancement > concentration
in priority order) with normalized inverse-SOC to produce a per-cell
`priority_score` and `priority_rank`, plus a `priority_map.tif`. Enable
for mitigation-targeting use cases.

**OCO-native fine grid** (`multiscale_fine_grid`, `fine_grid_res`)
Re-bins the SAME OCO/GOSAT retrievals at a finer resolution (default
0.02°) and writes `xco2_composite_fine.tif`. Enable when analyzing OCO
Target or Small Area Mapping acquisitions, where the ~0.1° merged grid
wastes the ~1.3 km native resolution.

---

## Output Files

| File                  | Type      | Contents                                      |
|-----------------------|-----------|-----------------------------------------------|
| `ch4_composite.tif`   | GeoTIFF   | Annual mean CH₄ (ppb) at chosen grid res      |
| `xco2_composite.tif`  | GeoTIFF   | Annual mean XCO₂ (ppm) merged OCO-2/3 + GOSAT |
| `ghg_hotspots.gpkg`   | GeoPackage| Cells > 90th percentile flagged as hotspots   |
| `soc_points.gpkg`     | GeoPackage| Your SOC/SIC field points (WB-corrected)      |
| `run_summary.txt`     | Text      | Run metadata and file paths                   |
| `run_config.json`     | JSON      | Full config snapshot for reproducibility      |

---

## Project Structure

```
ghg_mapper_plugin/
├── plugins/ghg_mapper/         # QGIS plugin (UI layer)
│   ├── ghg_mapper_plugin.py    # Plugin class (toolbar, menu)
│   ├── ghg_mapper_dialog.py    # Main dialog (4 tabs, no-code UI)
│   └── metadata.txt
├── src/ghg_mapper/             # Backend (testable without QGIS)
│   └── pipeline/
│       └── run_pipeline.py     # GEE extraction + composite + hotspot detection
├── tests/
├── environment.yml
└── pyproject.toml
```

---

## Extending the Plugin

The backend (`src/ghg_mapper/`) is plain Python — you can run it from the command line,
import it in a Jupyter notebook, or call it from another QGIS processing algorithm.

To add a new satellite source:
1. Add a `_stage_<satellite>()` function in `run_pipeline.py`
2. Add a checkbox for it in `ghg_mapper_dialog.py` (`_tab_setup`)
3. Register it in the `stages` list in `run_full_pipeline()`

---

## Impact Statement

GHG Mapper is designed to connect satellite-derived emission evidence to field-level
soil carbon stocks and management practices in smallholder agricultural systems.

**Estimated reach**: Agronomists, soil scientists, and environmental regulators working
across India's agricultural districts — particularly where direct GHG monitoring
infrastructure (flux towers, TCCON stations) is sparse.

**Carbon/soil benefit**: Enables identification of agricultural hotspot districts for
targeted soil carbon intervention, supporting Verra VCS VM0042 baseline development
and India's emerging CCTS framework.

**Connection to policy/market**: Outputs are structured to support MRV (Measurement,
Reporting, Verification) workflows under Verra VCS and India's domestic carbon market.

---

## Research Extensions (out of scope for v0.3)

The v0.3 Methodology tab covers Tier 1 + Tier 2 upgrades — defensible
improvements built from data and retrievals that are already in routine
operational production. Three further upgrades were identified during
spec authoring but are **intentionally excluded** because they are
research problems, not production methodology.

### AK-aware sensor fusion of OCO-2/3 and GOSAT ACOS

Proper fusion of dry-air mole-fraction retrievals across instruments
requires treating each sounding with its full averaging kernel and prior
profile (Rodgers & Connor 2003; O'Dell et al. 2018). Direct averaging
across instruments ignores differing vertical sensitivities and can bias
the result by ~0.3–0.5 ppm. v0.3 performs an inverse-variance-weighted
mean within a single instrument family only. AK-aware fusion is a
research activity requiring careful validation against TCCON and is left
to future work.

### Sentinel-2 plume detection for point-source CH₄

Varon et al. (2021) and Irakulis-Loitxate et al. (2022) demonstrated
point-source CH₄ detection from Sentinel-2 MSI over high-concentration
plumes using differential SWIR absorption. The method works for
super-emitters (> ~500 kg/h) but demands scene-by-scene manual QA,
cloud and shadow screening, and a spectral fitting codebase not shared
publicly. Integrating it into a no-code QGIS dialog with reproducible
results is a research project in its own right; v0.3 instead exposes
VIIRS/MODIS active-fire counts as the coarse combustion co-tracer.

### PRISMA / EMIT hyperspectral CH₄ imaging

Guanter et al. (2021) and Thorpe et al. (2023) showed PRISMA and EMIT
can map CH₄ column enhancements at ~30 m in the SWIR, enabling
field-level attribution of the plumes v0.3 detects at ~11 km.
Operational ingest requires matched-filter retrievals, orbital
tasking workflows, and scene-specific radiometric calibration — all
active research at JPL / DLR / ASI. v0.3 does not attempt this; the
cropland mask and upwind-source columns serve as a proxy for
field-level attribution until a turnkey hyperspectral retrieval is
available.

---

## License

MIT — free to use, modify, and distribute with attribution.
