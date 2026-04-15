# GHG Mapper — Agricultural India

**Multi-satellite GHG hotspot mapping with SOC/SIC ground-truth integration.**  
A QGIS plugin + Python backend for no-code satellite emission analysis.
## Compatibility

| Version | QGIS | Qt | Python |
|---------|------|----|--------|
| 1.1.0   | 4.0+ | Qt6 / PyQt6 | 3.12+ |
| 0.1.0   | 3.22–3.99 | Qt5 / PyQt5 | 3.9+ |
---

## What it does

GHG Mapper merges retrievals from TROPOMI (CH₄), OCO-2/OCO-3 (XCO₂), and GOSAT
(XCO₂ + XCH₄) into monthly composite hotspot maps over agricultural India.
It integrates your field-measured SOC and SIC values through a point-and-click
dialog — no Python coding required.
- Validates hotspots against CPCB CAAQMS ground stations — load your 
  5-year continuous monitoring CSV (PM10, PM2.5, SO2, NO, NO2, NOX, NH3, 
  CO, O3, Benzene, Toluene, Ethylbenzene, MP-Xylene, O-Xylene + met variables) 
  directly through the point-and-click UI. Outputs RMSE, mean bias, and 
  Pearson r per pollutant per station.

Built on the workflow described in:  
> Bander, T. (2024). *Multi-Satellite GHG Emission Hotspot Mapping over Agricultural India.*  


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

## Changelog

### v1.1.0 (April 2026)
- Ported to QGIS 4.0 / Qt6: all PyQt5 imports replaced with `qgis.PyQt`
- Fixed `QHeaderView.Stretch` → `QHeaderView.ResizeMode.Stretch` (Qt6 enum)
- Fixed `QFormLayout.setToolTip` → moved to parent `QGroupBox`
- Added **CAAQMS Validation tab**: load CPCB continuous monitoring CSV,
  preview 14 pollutants + 6 met variables, compute uncertainty metrics

### v0.1.0 (initial release)
- TROPOMI / OCO-2/3 / GOSAT composite pipeline via GEE
- SOC/SIC ground truth integration with Walkley-Black correction
- Hotspot detection at 90th percentile threshold

---

## License

MIT — free to use, modify, and distribute with attribution.
