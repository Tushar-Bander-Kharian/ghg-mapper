"""
ghg_mapper_dialog.py

Main dialog for GHG Mapper plugin.
Four tabs:
  1. Setup      — GEE authentication, study area, date range
  2. Ground Truth — SOC/SIC table input (no coding needed)
  3. Run         — trigger pipeline, live progress log
  4. Results     — load output layers into QGIS canvas

Design principle: every input has a tooltip explaining *what* it is and *why* it matters,
so a non-programmer agronomist can use it confidently.
"""
import os
import json
import csv
import traceback
from datetime import datetime

from PyQt5.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout, QTabWidget, QWidget,
    QLabel, QLineEdit, QPushButton, QComboBox, QSpinBox, QDoubleSpinBox,
    QTableWidget, QTableWidgetItem, QHeaderView, QTextEdit, QFileDialog,
    QGroupBox, QFormLayout, QDateEdit, QCheckBox, QProgressBar,
    QMessageBox, QSizePolicy, QSplitter
)
from PyQt5.QtCore import Qt, QDate, QThread, pyqtSignal, QTimer
from PyQt5.QtGui import QFont, QColor

from qgis.core import (
    QgsProject, QgsRasterLayer, QgsVectorLayer,
    QgsCoordinateReferenceSystem, QgsMessageLog, Qgis
)


# ---------------------------------------------------------------------------
# Worker thread — keeps UI responsive during long pipeline runs
# ---------------------------------------------------------------------------

class PipelineWorker(QThread):
    """Runs the backend pipeline in a background thread."""
    log_signal   = pyqtSignal(str)          # text message → log pane
    progress_signal = pyqtSignal(int)       # 0–100 → progress bar
    finished_signal = pyqtSignal(bool, str) # success, output_dir

    def __init__(self, config: dict):
        super().__init__()
        self.config = config

    def run(self):
        try:
            # Import here so QGIS startup is not slowed
            import sys, os
            # Add src/ to path so we can import ghg_mapper business logic
            src_path = os.path.abspath(
                os.path.join(os.path.dirname(__file__), "..", "..", "src")
            )
            if src_path not in sys.path:
                sys.path.insert(0, src_path)

            from ghg_mapper.pipeline.run_pipeline import run_full_pipeline
            run_full_pipeline(
                config=self.config,
                log_fn=self.log_signal.emit,
                progress_fn=self.progress_signal.emit,
            )
            self.finished_signal.emit(True, self.config["output_dir"])
        except Exception as e:
            self.log_signal.emit(f"\n[ERROR] {e}\n{traceback.format_exc()}")
            self.finished_signal.emit(False, "")


# ---------------------------------------------------------------------------
# Main Dialog
# ---------------------------------------------------------------------------

class GHGMapperDialog(QDialog):

    # Default India bounding box
    DEFAULT_BBOX = {"west": 68.0, "south": 8.0, "east": 97.5, "north": 37.6}

    def __init__(self, iface, parent=None):
        super().__init__(parent)
        self.iface = iface
        self.worker = None
        self.setWindowTitle("GHG Mapper — Agricultural India  v0.1")
        self.setMinimumSize(820, 680)
        self._build_ui()

    # ------------------------------------------------------------------
    # UI construction
    # ------------------------------------------------------------------

    def _build_ui(self):
        root = QVBoxLayout(self)

        # Banner
        banner = QLabel(
            "<b>🛰 GHG Mapper</b> — Multi-satellite CH₄ &amp; CO₂ hotspot mapping "
            "with SOC/SIC ground-truth integration"
        )
        banner.setWordWrap(True)
        banner.setStyleSheet("padding: 6px; background: #1a5276; color: white; border-radius: 4px;")
        root.addWidget(banner)

        # Tabs
        self.tabs = QTabWidget()
        self.tabs.addTab(self._tab_setup(),        "① Setup")
        self.tabs.addTab(self._tab_ground_truth(), "② Ground Truth (SOC / SIC)")
        self.tabs.addTab(self._tab_run(),          "③ Run Pipeline")
        self.tabs.addTab(self._tab_results(),      "④ Results")
        root.addWidget(self.tabs)

        # Bottom buttons
        btn_row = QHBoxLayout()
        btn_close = QPushButton("Close")
        btn_close.clicked.connect(self.close)
        btn_row.addStretch()
        btn_row.addWidget(btn_close)
        root.addLayout(btn_row)

    # ---- Tab 1: Setup ------------------------------------------------

    def _tab_setup(self):
        w = QWidget()
        layout = QVBoxLayout(w)

        # --- GEE Authentication ---
        gee_box = QGroupBox("Google Earth Engine Authentication")
        gee_form = QFormLayout(gee_box)

        self.gee_project_edit = QLineEdit()
        self.gee_project_edit.setPlaceholderText("e.g.  my-gee-project-id")
        self.gee_project_edit.setToolTip(
            "Your Google Cloud project ID linked to Earth Engine.\n"
            "Register free at https://earthengine.google.com/signup/"
        )
        gee_form.addRow("GEE Project ID:", self.gee_project_edit)

        gee_btn_row = QHBoxLayout()
        self.btn_gee_auth = QPushButton("Authenticate with GEE")
        self.btn_gee_auth.setToolTip(
            "Opens a browser window to authenticate your Google account.\n"
            "Only needed once per machine — credentials are saved locally."
        )
        self.btn_gee_auth.clicked.connect(self._on_gee_auth)
        self.gee_status_label = QLabel("Not authenticated")
        self.gee_status_label.setStyleSheet("color: #c0392b;")
        gee_btn_row.addWidget(self.btn_gee_auth)
        gee_btn_row.addWidget(self.gee_status_label)
        gee_btn_row.addStretch()
        gee_form.addRow("", gee_btn_row)
        layout.addWidget(gee_box)

        # --- Satellites to use ---
        sat_box = QGroupBox("Satellite Data Sources")
        sat_layout = QVBoxLayout(sat_box)
        self.chk_tropomi = QCheckBox("TROPOMI / Sentinel-5P  — CH₄  (daily, ~5.5×7 km)")
        self.chk_oco2    = QCheckBox("OCO-2  — XCO₂  (16-day, ~1.3×2.25 km)")
        self.chk_oco3    = QCheckBox("OCO-3  — XCO₂  (ISS orbit, variable)")
        self.chk_gosat   = QCheckBox("GOSAT  — XCO₂ + XCH₄  (3-day, ~10 km)")
        for chk in [self.chk_tropomi, self.chk_oco2, self.chk_oco3, self.chk_gosat]:
            chk.setChecked(True)
            sat_layout.addWidget(chk)
        layout.addWidget(sat_box)

        # --- Area of Interest ---
        aoi_box = QGroupBox("Area of Interest (decimal degrees, WGS 84)")
        aoi_form = QFormLayout(aoi_box)
        aoi_form.setToolTip(
            "Define your study area bounding box.\n"
            "Defaults to all of India. For district-level work, narrow this down\n"
            "to reduce GEE processing time and data volume."
        )
        d = self.DEFAULT_BBOX
        self.west_spin  = self._lat_lon_spin(d["west"],  -180, 180)
        self.east_spin  = self._lat_lon_spin(d["east"],  -180, 180)
        self.south_spin = self._lat_lon_spin(d["south"],  -90,  90)
        self.north_spin = self._lat_lon_spin(d["north"],  -90,  90)
        aoi_form.addRow("West longitude (°E):", self.west_spin)
        aoi_form.addRow("East longitude (°E):", self.east_spin)
        aoi_form.addRow("South latitude (°N):", self.south_spin)
        aoi_form.addRow("North latitude (°N):", self.north_spin)

        # Udaipur preset button
        btn_preset_udaipur = QPushButton("📍  Udaipur district")
        btn_preset_udaipur.clicked.connect(
            lambda: self._set_bbox(73.0, 73.9, 24.0, 25.0)
        )
        btn_preset_india = QPushButton("🗺  All India")
        btn_preset_india.clicked.connect(
            lambda: self._set_bbox(68.0, 97.5, 8.0, 37.6)
        )
        preset_row = QHBoxLayout()
        preset_row.addWidget(QLabel("Quick presets:"))
        preset_row.addWidget(btn_preset_udaipur)
        preset_row.addWidget(btn_preset_india)
        preset_row.addStretch()
        aoi_form.addRow("", preset_row)
        layout.addWidget(aoi_box)

        # --- Date range ---
        date_box = QGroupBox("Date Range")
        date_form = QFormLayout(date_box)
        self.date_start = QDateEdit(QDate(2023, 1, 1))
        self.date_start.setCalendarPopup(True)
        self.date_start.setToolTip("Start of analysis period.")
        self.date_end   = QDateEdit(QDate(2023, 12, 31))
        self.date_end.setCalendarPopup(True)
        self.date_end.setToolTip(
            "End of analysis period.\n"
            "Monthly composites will be generated for each month in this range."
        )
        date_form.addRow("Start date:", self.date_start)
        date_form.addRow("End date:",   self.date_end)
        layout.addWidget(date_box)

        # --- Output directory ---
        out_box = QGroupBox("Output Directory")
        out_row = QHBoxLayout(out_box)
        self.output_dir_edit = QLineEdit()
        self.output_dir_edit.setPlaceholderText("Select folder where results will be saved...")
        btn_browse_out = QPushButton("Browse…")
        btn_browse_out.clicked.connect(self._browse_output_dir)
        out_row.addWidget(self.output_dir_edit)
        out_row.addWidget(btn_browse_out)
        layout.addWidget(out_box)

        layout.addStretch()
        return w

    # ---- Tab 2: Ground Truth (SOC / SIC) ----------------------------

    def _tab_ground_truth(self):
        w = QWidget()
        layout = QVBoxLayout(w)

        info = QLabel(
            "<b>Enter your field-measured soil carbon values here.</b><br>"
            "Each row is one sampling point. You can also import a CSV file.<br>"
            "These values will be spatially joined to the satellite GHG grid (0.1°×0.1°) "
            "to compare carbon stocks against emission hotspots."
        )
        info.setWordWrap(True)
        info.setStyleSheet("padding: 6px; background: #eaf4fb; border-radius: 4px;")
        layout.addWidget(info)

        # --- Walkley-Black correction toggle ---
        wb_row = QHBoxLayout()
        self.chk_wb_correction = QCheckBox(
            "Apply Walkley-Black correction factor (×1.334) to SOC values"
        )
        self.chk_wb_correction.setChecked(True)
        self.chk_wb_correction.setToolTip(
            "Most Indian soil labs report Walkley-Black SOC WITHOUT the 1.334 recovery correction.\n"
            "Tick this if your SOC values come directly from a Walkley-Black titration\n"
            "and have NOT already been multiplied by 1.334.\n\n"
            "If your values are from dry combustion (CHN analyser) or already corrected, UNTICK this."
        )
        wb_row.addWidget(self.chk_wb_correction)
        wb_row.addStretch()
        layout.addLayout(wb_row)

        # --- Table ---
        self.soil_table = QTableWidget(0, 7)
        self.soil_table.setHorizontalHeaderLabels([
            "Sample ID",
            "Latitude (°N)",
            "Longitude (°E)",
            "Depth (cm)\ne.g. 0–15",
            "SOC (%)\nWalkley-Black or dry comb.",
            "SIC (%)\nRapid titration / calcimeter",
            "Notes",
        ])
        self.soil_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.soil_table.setToolTip(
            "SOC = Soil Organic Carbon  |  SIC = Soil Inorganic Carbon (as CaCO₃)\n"
            "Leave SIC blank if not measured — the pipeline will mark it as missing."
        )
        layout.addWidget(self.soil_table)

        # Table action buttons
        btn_row = QHBoxLayout()
        btn_add_row    = QPushButton("＋ Add row")
        btn_del_row    = QPushButton("－ Delete selected row")
        btn_import_csv = QPushButton("📂  Import from CSV…")
        btn_export_csv = QPushButton("💾  Export to CSV…")
        btn_add_row.clicked.connect(self._add_soil_row)
        btn_del_row.clicked.connect(self._del_soil_row)
        btn_import_csv.clicked.connect(self._import_soil_csv)
        btn_export_csv.clicked.connect(self._export_soil_csv)
        btn_add_row.setToolTip("Add a blank row to enter a new sample point manually.")
        btn_import_csv.setToolTip(
            "Import a CSV with columns: sample_id, latitude, longitude, depth, soc_pct, sic_pct, notes\n"
            "Column order matters; extra columns are ignored."
        )
        for b in [btn_add_row, btn_del_row, btn_import_csv, btn_export_csv]:
            btn_row.addWidget(b)
        btn_row.addStretch()
        layout.addLayout(btn_row)

        # --- Hyperspectral raster ---
        hyp_box = QGroupBox("Hyperspectral SOC Prediction Raster (optional)")
        hyp_form = QFormLayout(hyp_box)
        hyp_info = QLabel(
            "If you have a SOC prediction raster (GeoTIFF) from your PLSR/RF model "
            "on the Udaipur hyperspectral dataset, load it here. It will be resampled "
            "to the 0.1° grid and used as a continuous carbon surface layer."
        )
        hyp_info.setWordWrap(True)
        hyp_form.addRow(hyp_info)

        hyp_file_row = QHBoxLayout()
        self.hyp_soc_edit = QLineEdit()
        self.hyp_soc_edit.setPlaceholderText("Path to SOC prediction GeoTIFF…")
        btn_browse_soc = QPushButton("Browse…")
        btn_browse_soc.clicked.connect(lambda: self._browse_raster(self.hyp_soc_edit))
        hyp_file_row.addWidget(self.hyp_soc_edit)
        hyp_file_row.addWidget(btn_browse_soc)
        hyp_form.addRow("SOC raster:", hyp_file_row)

        hyp_sic_row = QHBoxLayout()
        self.hyp_sic_edit = QLineEdit()
        self.hyp_sic_edit.setPlaceholderText("Path to SIC prediction GeoTIFF (optional)…")
        btn_browse_sic = QPushButton("Browse…")
        btn_browse_sic.clicked.connect(lambda: self._browse_raster(self.hyp_sic_edit))
        hyp_sic_row.addWidget(self.hyp_sic_edit)
        hyp_sic_row.addWidget(btn_browse_sic)
        hyp_form.addRow("SIC raster:", hyp_sic_row)
        layout.addWidget(hyp_box)

        return w

    # ---- Tab 3: Run Pipeline -----------------------------------------

    def _tab_run(self):
        w = QWidget()
        layout = QVBoxLayout(w)

        # Config summary
        run_info = QLabel(
            "<b>Review settings, then click Run.</b><br>"
            "The pipeline will: (1) pull satellite data via GEE, "
            "(2) build monthly composites on a 0.1°×0.1° grid, "
            "(3) join your SOC/SIC points, "
            "(4) identify GHG hotspots, "
            "(5) save GeoTIFFs + shapefiles to your output folder."
        )
        run_info.setWordWrap(True)
        run_info.setStyleSheet("padding: 6px; background: #eafaf1; border-radius: 4px;")
        layout.addWidget(run_info)

        # CAAQMS option
        caaqms_box = QGroupBox("CAAQMS Validation (optional)")
        caaqms_form = QFormLayout(caaqms_box)
        caaqms_note = QLabel(
            "If you have downloaded CAAQMS CSV files from the CPCB portal "
            "(app.cpcbccr.com), point to the folder here. The pipeline will "
            "use NO₂ and CO as combustion co-tracers to validate satellite hotspots."
        )
        caaqms_note.setWordWrap(True)
        caaqms_form.addRow(caaqms_note)
        caaqms_row = QHBoxLayout()
        self.caaqms_dir_edit = QLineEdit()
        self.caaqms_dir_edit.setPlaceholderText("Folder containing CAAQMS CSV files (leave blank to skip)…")
        btn_browse_caaqms = QPushButton("Browse…")
        btn_browse_caaqms.clicked.connect(self._browse_caaqms_dir)
        caaqms_row.addWidget(self.caaqms_dir_edit)
        caaqms_row.addWidget(btn_browse_caaqms)
        caaqms_form.addRow("CAAQMS folder:", caaqms_row)
        layout.addWidget(caaqms_box)

        # Composite resolution
        grid_box = QGroupBox("Grid Settings")
        grid_form = QFormLayout(grid_box)
        self.grid_res_spin = QDoubleSpinBox()
        self.grid_res_spin.setRange(0.05, 1.0)
        self.grid_res_spin.setSingleStep(0.05)
        self.grid_res_spin.setValue(0.1)
        self.grid_res_spin.setDecimals(2)
        self.grid_res_spin.setToolTip(
            "Composite grid resolution in degrees.\n"
            "0.1° ≈ 11 km — recommended for all-India analysis.\n"
            "0.05° ≈ 5.5 km — finer, better matches TROPOMI pixel size, higher compute cost."
        )
        grid_form.addRow("Composite grid resolution (°):", self.grid_res_spin)
        layout.addWidget(grid_box)

        # Run / Stop buttons
        run_btn_row = QHBoxLayout()
        self.btn_run  = QPushButton("▶  Run Pipeline")
        self.btn_stop = QPushButton("■  Stop")
        self.btn_run.setStyleSheet(
            "QPushButton { background: #1a5276; color: white; font-weight: bold; padding: 8px 20px; border-radius: 4px; }"
            "QPushButton:hover { background: #154360; }"
        )
        self.btn_stop.setEnabled(False)
        self.btn_run.clicked.connect(self._on_run)
        self.btn_stop.clicked.connect(self._on_stop)
        run_btn_row.addWidget(self.btn_run)
        run_btn_row.addWidget(self.btn_stop)
        run_btn_row.addStretch()
        layout.addLayout(run_btn_row)

        # Progress bar
        self.progress_bar = QProgressBar()
        self.progress_bar.setValue(0)
        self.progress_bar.setTextVisible(True)
        layout.addWidget(self.progress_bar)

        # Log pane
        log_label = QLabel("Pipeline Log:")
        log_label.setFont(QFont("Consolas", 9))
        layout.addWidget(log_label)
        self.log_pane = QTextEdit()
        self.log_pane.setReadOnly(True)
        self.log_pane.setFont(QFont("Consolas", 9))
        self.log_pane.setStyleSheet("background: #1c2833; color: #aed6f1;")
        layout.addWidget(self.log_pane)

        return w

    # ---- Tab 4: Results ----------------------------------------------

    def _tab_results(self):
        w = QWidget()
        layout = QVBoxLayout(w)

        info = QLabel(
            "<b>Load output layers directly into your QGIS project.</b><br>"
            "After the pipeline finishes, use the buttons below to add layers to the map canvas."
        )
        info.setWordWrap(True)
        info.setStyleSheet("padding: 6px; background: #fef9e7; border-radius: 4px;")
        layout.addWidget(info)

        self.btn_load_ch4   = self._result_btn("🗺  Load CH₄ composite (GeoTIFF)",   self._load_ch4)
        self.btn_load_co2   = self._result_btn("🗺  Load XCO₂ composite (GeoTIFF)",  self._load_co2)
        self.btn_load_hotspot = self._result_btn("📌  Load hotspot shapefile",        self._load_hotspot)
        self.btn_load_soc   = self._result_btn("🟤  Load SOC point layer",            self._load_soc)
        self.btn_open_dir   = self._result_btn("📁  Open output folder in explorer",  self._open_output_dir)

        for btn in [self.btn_load_ch4, self.btn_load_co2,
                    self.btn_load_hotspot, self.btn_load_soc, self.btn_open_dir]:
            layout.addWidget(btn)

        layout.addStretch()

        # Quick stats panel
        stats_box = QGroupBox("Quick Statistics (last run)")
        stats_layout = QVBoxLayout(stats_box)
        self.stats_label = QLabel("Run the pipeline to see statistics here.")
        self.stats_label.setWordWrap(True)
        self.stats_label.setFont(QFont("Consolas", 9))
        stats_layout.addWidget(self.stats_label)
        layout.addWidget(stats_box)

        return w

    # ------------------------------------------------------------------
    # UI helpers
    # ------------------------------------------------------------------

    def _lat_lon_spin(self, default, min_val, max_val):
        s = QDoubleSpinBox()
        s.setRange(min_val, max_val)
        s.setDecimals(4)
        s.setSingleStep(0.5)
        s.setValue(default)
        return s

    def _result_btn(self, text, callback):
        btn = QPushButton(text)
        btn.clicked.connect(callback)
        btn.setStyleSheet("text-align: left; padding: 6px;")
        return btn

    def _set_bbox(self, west, east, south, north):
        self.west_spin.setValue(west)
        self.east_spin.setValue(east)
        self.south_spin.setValue(south)
        self.north_spin.setValue(north)

    # ------------------------------------------------------------------
    # File/dir browse callbacks
    # ------------------------------------------------------------------

    def _browse_output_dir(self):
        d = QFileDialog.getExistingDirectory(self, "Select Output Directory")
        if d:
            self.output_dir_edit.setText(d)

    def _browse_caaqms_dir(self):
        d = QFileDialog.getExistingDirectory(self, "Select CAAQMS CSV Folder")
        if d:
            self.caaqms_dir_edit.setText(d)

    def _browse_raster(self, line_edit):
        path, _ = QFileDialog.getOpenFileName(
            self, "Select GeoTIFF", "", "GeoTIFF (*.tif *.tiff);;All files (*)"
        )
        if path:
            line_edit.setText(path)

    # ------------------------------------------------------------------
    # GEE Authentication
    # ------------------------------------------------------------------

    def _on_gee_auth(self):
        try:
            import ee
            project = self.gee_project_edit.text().strip()
            if not project:
                QMessageBox.warning(self, "GEE Auth", "Please enter your GEE Project ID first.")
                return
            self.log("Authenticating with Google Earth Engine…")
            ee.Authenticate()
            ee.Initialize(project=project)
            self.gee_status_label.setText("✅  Authenticated")
            self.gee_status_label.setStyleSheet("color: #1e8449;")
            self.log("GEE authentication successful.")
        except ImportError:
            QMessageBox.critical(
                self, "GEE not installed",
                "The 'earthengine-api' Python package is not installed.\n\n"
                "Open OSGeo4W Shell and run:\n\n"
                "  pip install earthengine-api\n\n"
                "Then restart QGIS."
            )
        except Exception as e:
            self.gee_status_label.setText("❌  Failed")
            self.gee_status_label.setStyleSheet("color: #c0392b;")
            self.log(f"[ERROR] GEE auth failed: {e}")

    # ------------------------------------------------------------------
    # Soil table actions
    # ------------------------------------------------------------------

    def _add_soil_row(self):
        r = self.soil_table.rowCount()
        self.soil_table.insertRow(r)
        self.soil_table.setItem(r, 0, QTableWidgetItem(f"S{r+1:03d}"))

    def _del_soil_row(self):
        rows = sorted({i.row() for i in self.soil_table.selectedItems()}, reverse=True)
        for r in rows:
            self.soil_table.removeRow(r)

    def _import_soil_csv(self):
        path, _ = QFileDialog.getOpenFileName(
            self, "Import Soil CSV", "", "CSV files (*.csv);;All files (*)"
        )
        if not path:
            return
        try:
            with open(path, newline="", encoding="utf-8-sig") as f:
                reader = csv.reader(f)
                header = next(reader, None)
                for row_data in reader:
                    r = self.soil_table.rowCount()
                    self.soil_table.insertRow(r)
                    for col, val in enumerate(row_data[:7]):
                        self.soil_table.setItem(r, col, QTableWidgetItem(val.strip()))
            self.log(f"Imported soil data from {os.path.basename(path)}")
        except Exception as e:
            QMessageBox.warning(self, "Import failed", str(e))

    def _export_soil_csv(self):
        path, _ = QFileDialog.getSaveFileName(
            self, "Export Soil CSV", "soil_data.csv", "CSV files (*.csv)"
        )
        if not path:
            return
        headers = ["sample_id", "latitude", "longitude", "depth_cm",
                   "soc_pct", "sic_pct", "notes"]
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(headers)
            for r in range(self.soil_table.rowCount()):
                row_data = []
                for c in range(7):
                    item = self.soil_table.item(r, c)
                    row_data.append(item.text() if item else "")
                writer.writerow(row_data)
        self.log(f"Exported soil data to {os.path.basename(path)}")

    def _collect_soil_records(self):
        """Return soil table data as list of dicts."""
        records = []
        apply_wb = self.chk_wb_correction.isChecked()
        for r in range(self.soil_table.rowCount()):
            def cell(c):
                item = self.soil_table.item(r, c)
                return item.text().strip() if item else ""
            try:
                soc = float(cell(4)) if cell(4) else None
                if soc is not None and apply_wb:
                    soc = soc * 1.334
                sic = float(cell(5)) if cell(5) else None
                records.append({
                    "sample_id": cell(0),
                    "lat": float(cell(1)),
                    "lon": float(cell(2)),
                    "depth_cm": cell(3),
                    "soc_pct": soc,
                    "sic_pct": sic,
                    "notes": cell(6),
                })
            except ValueError:
                pass  # skip rows with missing/bad lat-lon
        return records

    # ------------------------------------------------------------------
    # Run / Stop pipeline
    # ------------------------------------------------------------------

    def _build_config(self):
        return {
            "gee_project":   self.gee_project_edit.text().strip(),
            "satellites": {
                "tropomi": self.chk_tropomi.isChecked(),
                "oco2":    self.chk_oco2.isChecked(),
                "oco3":    self.chk_oco3.isChecked(),
                "gosat":   self.chk_gosat.isChecked(),
            },
            "bbox": {
                "west":  self.west_spin.value(),
                "east":  self.east_spin.value(),
                "south": self.south_spin.value(),
                "north": self.north_spin.value(),
            },
            "date_start":   self.date_start.date().toString("yyyy-MM-dd"),
            "date_end":     self.date_end.date().toString("yyyy-MM-dd"),
            "output_dir":   self.output_dir_edit.text().strip(),
            "grid_res":     self.grid_res_spin.value(),
            "caaqms_dir":   self.caaqms_dir_edit.text().strip() or None,
            "soil_records": self._collect_soil_records(),
            "hyp_soc_raster": self.hyp_soc_edit.text().strip() or None,
            "hyp_sic_raster": self.hyp_sic_edit.text().strip() or None,
        }

    def _on_run(self):
        config = self._build_config()

        # Validate
        errors = []
        if not config["gee_project"]:
            errors.append("GEE Project ID is required (Setup tab).")
        if not config["output_dir"]:
            errors.append("Output directory is required (Setup tab).")
        if not any(config["satellites"].values()):
            errors.append("Select at least one satellite (Setup tab).")
        if errors:
            QMessageBox.warning(self, "Cannot run — missing settings",
                                "\n".join(f"• {e}" for e in errors))
            return

        os.makedirs(config["output_dir"], exist_ok=True)

        self.log_pane.clear()
        self.log(f"Starting pipeline  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        self.log(f"AOI: W{config['bbox']['west']} E{config['bbox']['east']} "
                 f"S{config['bbox']['south']} N{config['bbox']['north']}")
        self.log(f"Period: {config['date_start']} → {config['date_end']}")
        self.log(f"Soil records provided: {len(config['soil_records'])}")
        self.log("─" * 60)

        self.btn_run.setEnabled(False)
        self.btn_stop.setEnabled(True)
        self.progress_bar.setValue(0)
        self.tabs.setCurrentIndex(2)  # show Run tab

        self.worker = PipelineWorker(config)
        self.worker.log_signal.connect(self.log)
        self.worker.progress_signal.connect(self.progress_bar.setValue)
        self.worker.finished_signal.connect(self._on_finished)
        self.worker.start()

    def _on_stop(self):
        if self.worker and self.worker.isRunning():
            self.worker.terminate()
            self.log("\n[STOPPED by user]")
            self._reset_run_buttons()

    def _on_finished(self, success: bool, output_dir: str):
        self._reset_run_buttons()
        if success:
            self.progress_bar.setValue(100)
            self.log("\n✅  Pipeline complete.")
            self.log(f"Outputs saved to: {output_dir}")
            self.stats_label.setText(
                f"Last run: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n"
                f"Output folder: {output_dir}"
            )
            self.tabs.setCurrentIndex(3)  # jump to Results tab
            QMessageBox.information(
                self, "Done",
                "Pipeline finished successfully.\n\n"
                "Switch to the Results tab to load layers into QGIS."
            )
        else:
            self.log("\n❌  Pipeline failed — see log above for details.")
            QMessageBox.critical(self, "Pipeline failed",
                                 "An error occurred. Check the pipeline log for details.")

    def _reset_run_buttons(self):
        self.btn_run.setEnabled(True)
        self.btn_stop.setEnabled(False)

    # ------------------------------------------------------------------
    # Result layer loading
    # ------------------------------------------------------------------

    def _output_path(self, filename):
        return os.path.join(self.output_dir_edit.text().strip(), filename)

    def _load_raster(self, filename, layer_name):
        path = self._output_path(filename)
        if not os.path.exists(path):
            QMessageBox.warning(self, "File not found",
                                f"{filename} not found in output folder.\n"
                                "Have you run the pipeline yet?")
            return
        layer = QgsRasterLayer(path, layer_name)
        if layer.isValid():
            QgsProject.instance().addMapLayer(layer)
            self.iface.messageBar().pushMessage(
                "GHG Mapper", f"Loaded: {layer_name}", level=Qgis.Success
            )
        else:
            QMessageBox.critical(self, "Load failed", f"Could not load {filename}")

    def _load_vector(self, filename, layer_name):
        path = self._output_path(filename)
        if not os.path.exists(path):
            QMessageBox.warning(self, "File not found",
                                f"{filename} not found.\nHave you run the pipeline yet?")
            return
        layer = QgsVectorLayer(path, layer_name, "ogr")
        if layer.isValid():
            QgsProject.instance().addMapLayer(layer)
            self.iface.messageBar().pushMessage(
                "GHG Mapper", f"Loaded: {layer_name}", level=Qgis.Success
            )
        else:
            QMessageBox.critical(self, "Load failed", f"Could not load {filename}")

    def _load_ch4(self):
        self._load_raster("ch4_composite.tif", "CH₄ Composite (TROPOMI)")

    def _load_co2(self):
        self._load_raster("xco2_composite.tif", "XCO₂ Composite (OCO-2/3 + GOSAT)")

    def _load_hotspot(self):
        self._load_vector("ghg_hotspots.gpkg", "GHG Hotspots")

    def _load_soc(self):
        self._load_vector("soc_points.gpkg", "SOC/SIC Field Points")

    def _open_output_dir(self):
        import subprocess, sys
        d = self.output_dir_edit.text().strip()
        if not d or not os.path.isdir(d):
            QMessageBox.warning(self, "No output folder", "Set and run the pipeline first.")
            return
        if sys.platform == "win32":
            os.startfile(d)
        elif sys.platform == "darwin":
            subprocess.Popen(["open", d])
        else:
            subprocess.Popen(["xdg-open", d])

    # ------------------------------------------------------------------
    # Logging
    # ------------------------------------------------------------------

    def log(self, msg: str):
        self.log_pane.append(msg)
        self.log_pane.verticalScrollBar().setValue(
            self.log_pane.verticalScrollBar().maximum()
        )
        QgsMessageLog.logMessage(msg, "GHG Mapper", Qgis.Info)
