# GHG Mapper — Agricultural India

**Multi-satellite GHG hotspot mapping with SOC/SIC ground-truth integration.**  
A QGIS plugin + Python backend for no-code satellite emission analysis.

## Compatibility

| Version | QGIS | Qt | Python |
|---------|------|----|--------|
| 1.1.2   | 4.0+ | Qt6 / PyQt6 | 3.12+ |
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

## Account Setup (required before first run)

The plugin pulls data from three separate services. Each requires a free account.

### Google Earth Engine (TROPOMI CH₄)

1. Sign up at https://earthengine.google.com/signup/
2. Create a Cloud project at https://console.cloud.google.com/
3. In the plugin **Setup** tab enter your project ID (e.g. `my-gee-project`) and click **Authenticate**.

### NASA EarthData (OCO-2, OCO-3, GOSAT XCO₂)

OCO-2/3 and GOSAT XCO₂ data are served by NASA GES DISC and require an
EarthData account **plus** explicit approval of the GES DISC application.
Both steps are mandatory — skipping the application approval causes a
`401 Pre-authorization required` error even with otherwise correct credentials.

**Step 1 — Create a free EarthData account**

Go to https://urs.earthdata.nasa.gov/users/new and register.
Keep your username and password — you will enter them in the plugin Setup tab.

**Step 2 — Approve the GES DISC application**

While logged in to EarthData, open this URL in your browser:

```
https://urs.earthdata.nasa.gov/approve_app?client_id=e2WVk8Pw6weeLUKZYOxvTQ
```

Click **Approve**. This grants the plugin permission to download OCO-2/3 and
GOSAT granules from `data.gesdisc.earthdata.nasa.gov`. You only need to do
this once per EarthData account.

**Step 3 — Enter credentials in the plugin**

In the **Setup** tab, fill in:
- **EarthData username** — your `urs.earthdata.nasa.gov` username
- **EarthData password** — your `urs.earthdata.nasa.gov` password

The plugin writes these to `~/.netrc` automatically so the download session
can authenticate. The file is created with permission `600` (owner-read only).

**Datasets downloaded**

| Dataset | Short name | Version | Variable |
|---------|-----------|---------|----------|
| OCO-2 L2 Lite FP | `OCO2_L2_Lite_FP` | 11.2r | XCO₂, quality_flag = 0 |
| OCO-3 L2 Lite FP | `OCO3_L2_Lite_FP` | 10.4r | XCO₂, quality_flag = 0 |
| GOSAT ACOS L2 Lite | `ACOS_L2_Lite_FP` | 9r | XCO₂, quality_flag = 0 |

### NIES GOSAT Portal (GOSAT XCH₄, optional)

GOSAT XCH₄ (TANSO-FTS SWIR) is distributed via SFTP by NIES Japan.

1. Register at https://prdct.gosat-2.nies.go.jp/en/aboutdata/directsftpaccess.html
2. After approval (usually 1–3 business days) you receive SFTP credentials by email.
3. Enter them in the plugin **Setup** tab under **NIES GOSAT Login**.

NIES credentials are optional. The pipeline runs with ≥2 available sources, so
TROPOMI + any one EarthData satellite is sufficient to start.

---

## Quick Start

### 1. Install Python packages

Open **OSGeo4W Shell** (installed with QGIS — search Start menu):

```bash
pip install requests pydap paramiko
```

`h5py` and `netCDF4` are optional — the plugin falls back to GDAL (bundled
with QGIS) to read OCO granule files if neither is available.

### 2. Install the QGIS plugin

Copy the `ghg_mapper_plugin/` folder to your QGIS plugins directory:

- **Windows**: `%APPDATA%\QGIS\QGIS4\profiles\default\python\plugins\`
- **Linux/Mac**: `~/.local/share/QGIS/QGIS4/profiles/default/python/plugins/`

Then in QGIS: *Plugins → Manage and Install Plugins → Installed → ✅ GHG Mapper*

### 3. Open the dialog

**Raster → GHG Mapper → Open GHG Mapper**

### 4. Fill in the Setup tab

- GEE project ID → click **Authenticate**
- EarthData username and password (see Account Setup above)
- NIES username and password (optional — only needed for GOSAT XCH₄)

The green/red availability indicator shows how many data sources are credentialled.
The pipeline requires at least 2 available sources before it will run.

### 5. Enter SOC/SIC values (optional)

On the **Ground Truth** tab, type or import field sample values.
Tick the Walkley-Black checkbox if your SOC was measured by the Walkley-Black
method — it applies the ×1.334 recovery correction automatically.

### 6. Run

Set the date range and output folder on the **Run** tab, then click **▶ Run Pipeline**.
Outputs appear in your chosen folder when the pipeline completes.

---

## Output Files

| File                      | Type       | Contents                                       |
|---------------------------|------------|------------------------------------------------|
| `ch4_composite.tif`       | GeoTIFF    | Mean CH₄ (ppb) from TROPOMI                    |
| `xco2_composite.tif`      | GeoTIFF    | Mean XCO₂ (ppm) merged OCO-2/3 + GOSAT ACOS   |
| `gosat_ch4_composite.tif` | GeoTIFF    | Mean XCH₄ (ppb) from GOSAT TANSO-FTS (NIES)   |
| `ghg_hotspots.gpkg`       | GeoPackage | Cells > 90th percentile flagged as hotspots    |
| `soc_points.gpkg`         | GeoPackage | SOC/SIC field points (Walkley-Black corrected) |
| `run_summary.txt`         | Text       | Run metadata and file paths                    |
| `run_config.json`         | JSON       | Full config snapshot for reproducibility       |

---

## Project Structure

```
ghg_mapper_plugin/
├── ghg_mapper_plugin.py    # Plugin class (toolbar, menu)
├── ghg_mapper_dialog.py    # Main dialog (4 tabs, no-code UI)
├── metadata.txt
├── src/ghg_mapper/
│   └── pipeline/
│       └── run_pipeline.py # Satellite download + composite + hotspot detection
├── environment.yml
└── pyproject.toml
```

---

## Troubleshooting

### 401 Unauthorized when downloading OCO-2/3

You have not approved the GES DISC application on your EarthData account.
Visit the URL in the Account Setup section above and click Approve, then re-run.

### "0 granules found" for OCO-2/3 or GOSAT

CMR search returned no results. Common causes:
- Date range has no satellite overpasses over the India AOI — try a longer period (3+ months)
- Wrong product version — versions are pinned as constants in `run_pipeline.py`
- EarthData account not yet approved for GES DISC (see above)

### "Cannot read NC4 file" error

`h5py`, `netCDF4`, and `osgeo.gdal` were all unavailable. This should not happen
inside QGIS because GDAL is always bundled. If running outside QGIS:

```bash
pip install h5py
```

### NIES SFTP connects but finds no data

The plugin logs the full SFTP directory tree (three levels deep) on every run.
Check the pipeline log panel for lines starting with `GOSAT XCH₄:` to see what
directories are visible on the server.

---

## Extending the Plugin

The backend (`src/ghg_mapper/`) is plain Python — you can run it from the command line,
import it in a Jupyter notebook, or call it from another QGIS processing algorithm.

To add a new satellite source:
1. Add a `_stage_<satellite>()` function in `run_pipeline.py`
2. Add a checkbox and credential fields in `ghg_mapper_dialog.py` (`_tab_setup`)
3. Wire the credential fields through `_update_sat_availability()` and `_build_config()`

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

## Changelog

### v1.1.2 (April 2026)
- **Fix NC4 reader**: added `osgeo.gdal` as third fallback in `_read_nc4_oco_vars`
  so OCO granules parse inside QGIS without any `pip install` (GDAL is always bundled)
- **NIES probe**: deepened SFTP directory probe to three levels and stopped
  swallowing exceptions so the correct data path appears in the pipeline log

### v1.1.1 (April 2026)
- **Fix OCO-2/3 401 Unauthorized**: switched from OPeNDAP streaming (pydap) to
  direct HTTPS download + file reading; pydap session cookies from `setup_session`
  did not persist across internal `.dods` data-fetch requests
- Updated NASA GES DISC hostname from `gesdisc.eosdis.nasa.gov` to
  `data.gesdisc.earthdata.nasa.gov` following NASA's domain migration (April 2024)
- Added `check_url` to `pydap.cas.urs.setup_session` so URS cookies are scoped
  to the correct data server before the first granule request
- Added GOSAT ACOS XCO₂ (`ACOS_L2_Lite_FP`) support alongside OCO-2/3
- Added GOSAT XCH₄ via NIES SFTP portal (`_stage_gosat_ch4_nies`)
- Added NIES credentials UI and ≥2-source validation gate before pipeline run

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
