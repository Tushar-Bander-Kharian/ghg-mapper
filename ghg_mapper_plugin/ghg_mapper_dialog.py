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

from qgis.PyQt.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout, QTabWidget, QWidget,
    QLabel, QLineEdit, QPushButton, QComboBox, QSpinBox, QDoubleSpinBox,
    QTableWidget, QTableWidgetItem, QHeaderView, QTextEdit, QFileDialog,
    QGroupBox, QFormLayout, QDateEdit, QCheckBox, QProgressBar,
    QMessageBox, QSizePolicy, QSplitter
)
from qgis.PyQt.QtCore import Qt, QDate, QThread, pyqtSignal, QTimer, QSettings
from qgis.PyQt.QtGui import QFont, QColor

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
            import sys, os, importlib.util
            _run_pipeline_path = os.path.join(
                os.path.dirname(os.path.abspath(__file__)),
                'src', 'ghg_mapper', 'pipeline', 'run_pipeline.py'
            )
            _spec = importlib.util.spec_from_file_location(
                "ghg_mapper.pipeline.run_pipeline", _run_pipeline_path
            )
            _mod = importlib.util.module_from_spec(_spec)
            sys.modules["ghg_mapper.pipeline.run_pipeline"] = _mod
            sys.modules["ghg_mapper"] = _mod
            sys.modules["ghg_mapper.pipeline"] = _mod
            _spec.loader.exec_module(_mod)
            run_full_pipeline = _mod.run_full_pipeline
            PipelineConfig    = _mod.PipelineConfig
            bbox = self.config["bbox"]
            sats = self.config.get("satellites", {})
            cfg = PipelineConfig(
                start_date       = self.config["date_start"],
                end_date         = self.config["date_end"],
                aoi_west         = bbox["west"],
                aoi_east         = bbox["east"],
                aoi_south        = bbox["south"],
                aoi_north        = bbox["north"],
                output_dir       = self.config["output_dir"],
                gee_project      = self.config["gee_project"],
                grid_res         = self.config.get("grid_res", 0.1),
                use_tropomi      = sats.get("tropomi", True),
                use_oco2         = sats.get("oco2", True),
                use_oco3         = sats.get("oco3", True),
                use_gosat        = sats.get("gosat", True) or sats.get("gosat_ch4", True),
                earthdata_user   = self.config.get("earthdata_user"),
                earthdata_pass   = self.config.get("earthdata_pass"),
                nies_user        = self.config.get("nies_user"),
                nies_pass        = self.config.get("nies_pass"),
                soc_records      = self.config.get("soil_records", []),
                caaqms_csv       = self.config.get("caaqms_dir", None),
                wb_correction    = True,
            )
            def _progress(pct, msg):
                self.progress_signal.emit(pct)
                self.log_signal.emit(msg)
            run_full_pipeline(cfg, progress_fn=_progress)
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

    SETTINGS_ORG  = "GHGMapper"
    SETTINGS_APP  = "ghg_mapper"

    def __init__(self, iface, parent=None):
        super().__init__(parent)
        self.iface = iface
        self.worker = None
        self.setWindowTitle("GHG Mapper — Agricultural India  v0.1")
        self.setMinimumSize(820, 680)
        self._build_ui()
        self._load_settings()      # restore saved credentials & paths

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
        self.tabs.addTab(self._tab_caaqms(),       "③ CAAQMS Validation")
        self.tabs.addTab(self._tab_run(),          "④ Run Pipeline")
        self.tabs.addTab(self._tab_results(),      "⑤ Results")
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

        # --- NASA EarthData (for OCO-2/3 via PyDAP / OPeNDAP) ---
        nasa_box = QGroupBox("NASA EarthData Login (required for OCO-2 / OCO-3 XCO₂)")
        nasa_form = QFormLayout(nasa_box)

        nasa_info = QLabel(
            "OCO-2 and OCO-3 data are <b>not</b> in Google Earth Engine. "
            "They are streamed from NASA GES DISC via OPeNDAP using your free EarthData account "
            "(same method as <a href='https://github.com/sagarlimbu0/NASA-OCO2-OCO3'>"
            "NASA-OCO2-OCO3</a>)."
        )
        nasa_info.setWordWrap(True)
        nasa_info.setOpenExternalLinks(True)
        nasa_form.addRow(nasa_info)

        self.earthdata_user_edit = QLineEdit()
        self.earthdata_user_edit.setPlaceholderText("NASA EarthData username…")
        self.earthdata_user_edit.setToolTip(
            "Your NASA EarthData username.\n"
            "Register free at https://urs.earthdata.nasa.gov/\n\n"
            "Required packages (OSGeo4W Shell):\n"
            "  pip install pydap requests"
        )
        nasa_form.addRow("EarthData username:", self.earthdata_user_edit)

        self.earthdata_pass_edit = QLineEdit()
        self.earthdata_pass_edit.setPlaceholderText("NASA EarthData password…")
        self.earthdata_pass_edit.setEchoMode(QLineEdit.EchoMode.Password)
        nasa_form.addRow("EarthData password:", self.earthdata_pass_edit)

        # Try to pre-fill from ~/.netrc (same convention as the reference repo)
        netrc_row = QHBoxLayout()
        btn_load_netrc = QPushButton("Load from ~/.netrc")
        btn_load_netrc.setToolTip(
            "Auto-fill credentials from your ~/.netrc file.\n"
            "Add an entry like:\n"
            "  machine urs.earthdata.nasa.gov\n"
            "  login YOUR_USERNAME\n"
            "  password YOUR_PASSWORD"
        )
        btn_load_netrc.clicked.connect(self._load_earthdata_netrc)
        self.netrc_status_lbl = QLabel("")
        netrc_row.addWidget(btn_load_netrc)
        netrc_row.addWidget(self.netrc_status_lbl)
        netrc_row.addStretch()
        nasa_form.addRow("", netrc_row)

        nasa_link = QLabel(
            '<a href="https://urs.earthdata.nasa.gov/">Register / manage account at urs.earthdata.nasa.gov</a>'
        )
        nasa_link.setOpenExternalLinks(True)
        nasa_form.addRow("", nasa_link)
        layout.addWidget(nasa_box)
        self.earthdata_user_edit.textChanged.connect(self._update_sat_availability)
        self.earthdata_pass_edit.textChanged.connect(self._update_sat_availability)

        # --- NIES GOSAT XCH₄ credentials ---
        nies_box = QGroupBox("NIES GOSAT Login (optional — for GOSAT XCH₄)")
        nies_form = QFormLayout(nies_box)

        nies_info = QLabel(
            "GOSAT XCH₄ data is provided by Japan's National Institute for Environmental Studies (NIES) "
            "via SFTP. Register free at "
            "<a href='https://data2.gosat.nies.go.jp/'>data2.gosat.nies.go.jp</a>. "
            "Your login is your registered <b>email address</b> and the password you chose. "
            "Requires: <code>pip install paramiko</code>"
        )
        nies_info.setWordWrap(True)
        nies_info.setOpenExternalLinks(True)
        nies_form.addRow(nies_info)

        self.nies_user_edit = QLineEdit()
        self.nies_user_edit.setPlaceholderText("NIES registered email address…")
        self.nies_user_edit.setToolTip(
            "The email address you used to register at data2.gosat.nies.go.jp.\n"
            "This is your SFTP username for prdct.gosat-2.nies.go.jp."
        )
        nies_form.addRow("NIES username (email):", self.nies_user_edit)

        self.nies_pass_edit = QLineEdit()
        self.nies_pass_edit.setPlaceholderText("NIES password…")
        self.nies_pass_edit.setEchoMode(QLineEdit.EchoMode.Password)
        self.nies_pass_edit.setToolTip("Password you set when registering on the NIES GOSAT portal.")
        nies_form.addRow("NIES password:", self.nies_pass_edit)

        nies_status_row = QHBoxLayout()
        btn_test_nies = QPushButton("Test NIES connection")
        btn_test_nies.setToolTip("Verify SFTP credentials by connecting to prdct.gosat-2.nies.go.jp.")
        btn_test_nies.clicked.connect(self._test_nies_connection)
        self.nies_status_lbl = QLabel("")
        nies_status_row.addWidget(btn_test_nies)
        nies_status_row.addWidget(self.nies_status_lbl)
        nies_status_row.addStretch()
        nies_form.addRow("", nies_status_row)

        nies_link = QLabel(
            '<a href="https://data2.gosat.nies.go.jp/">Register at data2.gosat.nies.go.jp</a>'
            '  |  '
            '<a href="https://prdct.gosat-2.nies.go.jp/en/aboutdata/">SFTP access guide</a>'
        )
        nies_link.setOpenExternalLinks(True)
        nies_form.addRow("", nies_link)
        layout.addWidget(nies_box)
        self.nies_user_edit.textChanged.connect(self._update_sat_availability)
        self.nies_pass_edit.textChanged.connect(self._update_sat_availability)

        # --- Satellites to use ---
        sat_box = QGroupBox("Satellite Data Sources  (pipeline requires ≥ 2 enabled sources)")
        sat_layout = QVBoxLayout(sat_box)

        avail_note = QLabel(
            "<i>Sources marked <b>[GEE]</b> only need a GEE project. "
            "Sources marked <b>[EarthData]</b> need NASA credentials above. "
            "Sources marked <b>[NIES]</b> need NIES credentials above.</i>"
        )
        avail_note.setWordWrap(True)
        sat_layout.addWidget(avail_note)

        self.chk_tropomi = QCheckBox("✅  TROPOMI / Sentinel-5P  — CH₄  (daily, ~5.5×7 km)  [GEE]")
        self.chk_oco2    = QCheckBox("✅  OCO-2  — XCO₂  (16-day, ~1.3×2.25 km)  [EarthData]")
        self.chk_oco3    = QCheckBox("✅  OCO-3  — XCO₂  (ISS orbit, variable)  [EarthData]")
        self.chk_gosat   = QCheckBox("✅  GOSAT  — XCO₂ (ACOS/GES DISC)  [EarthData]")
        self.chk_gosat_ch4 = QCheckBox("✅  GOSAT  — XCH₄ (NIES TANSO-FTS)  [NIES]")

        self.chk_tropomi.setToolTip("Sentinel-5P TROPOMI methane (CH₄). Requires only GEE access.")
        self.chk_oco2.setToolTip("OCO-2 XCO₂. Requires NASA EarthData credentials.")
        self.chk_oco3.setToolTip("OCO-3 XCO₂ from ISS. Requires NASA EarthData credentials.")
        self.chk_gosat.setToolTip("GOSAT ACOS L2 XCO₂ from NASA GES DISC. Requires EarthData credentials.")
        self.chk_gosat_ch4.setToolTip(
            "GOSAT TANSO-FTS XCH₄ from NIES SFTP portal.\n"
            "Requires NIES credentials and: pip install paramiko"
        )
        for chk in [self.chk_tropomi, self.chk_oco2, self.chk_oco3,
                    self.chk_gosat, self.chk_gosat_ch4]:
            chk.setChecked(True)
            sat_layout.addWidget(chk)

        self.sat_avail_label = QLabel("")
        self.sat_avail_label.setWordWrap(True)
        sat_layout.addWidget(self.sat_avail_label)

        # update availability label when any checkbox or credential changes
        for chk in [self.chk_tropomi, self.chk_oco2, self.chk_oco3,
                    self.chk_gosat, self.chk_gosat_ch4]:
            chk.stateChanged.connect(self._update_sat_availability)
        layout.addWidget(sat_box)

        # --- Area of Interest ---
        aoi_box = QGroupBox("Area of Interest (decimal degrees, WGS 84)")
        aoi_form = QFormLayout(aoi_box)
        aoi_box.setToolTip(
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
        self.soil_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
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
    
    def _tab_caaqms(self):
        """③ CAAQMS Validation tab — load station CSV, preview, compute uncertainty."""
        import os, csv
        from qgis.PyQt.QtWidgets import (
            QWidget, QVBoxLayout, QHBoxLayout, QGroupBox, QFormLayout,
            QLabel, QPushButton, QLineEdit, QFileDialog, QTableWidget,
            QTableWidgetItem, QHeaderView, QTextEdit, QCheckBox,
            QComboBox, QSplitter, QSizePolicy, QMessageBox
        )
        from qgis.PyQt.QtCore import Qt
        from qgis.PyQt.QtGui import QFont, QColor

        # ── column schema ──────────────────────────────────────────────────────
        POLLUTANTS = [
            "PM10", "PM2.5", "SO2", "NO", "NO2", "NOX",
            "NH3", "CO", "O3", "Benzene", "Toluene",
            "Ethylbenzene", "MP_XYLENE", "O_XYLENE"
        ]
        MET_VARS = ["Temp", "RH", "WS", "BP", "Rain_Gauge", "SR"]
        ALL_COLS = ["Station", "Lat", "Lon", "Date"] + POLLUTANTS + MET_VARS

        widget = QWidget()
        root = QVBoxLayout(widget)
        root.setSpacing(8)

        # ── header ─────────────────────────────────────────────────────────────
        hdr = QLabel("CAAQMS / CPCB Ground Station Validation")
        hdr_font = QFont()
        hdr_font.setBold(True)
        hdr_font.setPointSize(11)
        hdr.setFont(hdr_font)
        root.addWidget(hdr)

        sub = QLabel(
            "Load your 5-year CPCB continuous monitoring CSV. "
            "The tool spatially matches each station to the nearest GHG hotspot "
            "grid cell and computes uncertainty metrics for all 14 pollutants + 6 met variables."
        )
        sub.setWordWrap(True)
        root.addWidget(sub)

        # ── file loader ────────────────────────────────────────────────────────
        file_box = QGroupBox("1 · Load CAAQMS CSV")
        file_layout = QFormLayout(file_box)

        file_row = QHBoxLayout()
        self.caaqms_path = QLineEdit()
        self.caaqms_path.setPlaceholderText("Path to your CAAQMS .csv file …")
        self.caaqms_path.setReadOnly(True)
        btn_browse_caaqms = QPushButton("Browse …")

        def browse_caaqms():
            path, _ = QFileDialog.getOpenFileName(
                widget, "Open CAAQMS CSV", "", "CSV files (*.csv);;All files (*)"
            )
            if path:
                self.caaqms_path.setText(path)
                _load_caaqms_preview(path)

        btn_browse_caaqms.clicked.connect(browse_caaqms)
        file_row.addWidget(self.caaqms_path)
        file_row.addWidget(btn_browse_caaqms)
        file_layout.addRow("CSV file:", file_row)

        # date range detected from file
        self.caaqms_date_range_lbl = QLabel("—")
        file_layout.addRow("Detected date range:", self.caaqms_date_range_lbl)

        self.caaqms_stations_lbl = QLabel("—")
        file_layout.addRow("Stations found:", self.caaqms_stations_lbl)

        self.caaqms_rows_lbl = QLabel("—")
        file_layout.addRow("Total records:", self.caaqms_rows_lbl)

        root.addWidget(file_box)

        # ── data preview table ─────────────────────────────────────────────────
        preview_box = QGroupBox("2 · Data Preview (first 100 rows)")
        preview_layout = QVBoxLayout(preview_box)

        self.caaqms_preview = QTableWidget(0, len(ALL_COLS))
        self.caaqms_preview.setHorizontalHeaderLabels(ALL_COLS)
        self.caaqms_preview.horizontalHeader().setSectionResizeMode(
            QHeaderView.ResizeMode.ResizeToContents
        )
        self.caaqms_preview.setAlternatingRowColors(True)
        self.caaqms_preview.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        self.caaqms_preview.setMinimumHeight(180)
        preview_layout.addWidget(self.caaqms_preview)
        root.addWidget(preview_box)

        # ── uncertainty options ────────────────────────────────────────────────
        unc_box = QGroupBox("3 · Uncertainty & Validation Options")
        unc_form = QFormLayout(unc_box)

        self.caaqms_pollutant_sel = QComboBox()
        self.caaqms_pollutant_sel.addItems(["All pollutants"] + POLLUTANTS)
        unc_form.addRow("Primary pollutant for GHG correlation:", self.caaqms_pollutant_sel)

        self.caaqms_chk_rmse    = QCheckBox("RMSE  (root mean square error vs satellite composite)")
        self.caaqms_chk_bias    = QCheckBox("Mean bias  (station − satellite)")
        self.caaqms_chk_corr    = QCheckBox("Pearson r  (temporal correlation)")
        self.caaqms_chk_spatial = QCheckBox("Spatial interpolation  (IDW station values → raster)")
        self.caaqms_chk_wind    = QCheckBox("Wind-weighted dispersion  (uses WS column)")
        for chk in [self.caaqms_chk_rmse, self.caaqms_chk_bias,
                    self.caaqms_chk_corr, self.caaqms_chk_spatial,
                    self.caaqms_chk_wind]:
            chk.setChecked(True)
            unc_form.addRow("", chk)

        self.caaqms_chk_met = QCheckBox(
            "Include met co-variates (Temp, RH, WS, BP) in uncertainty regression"
        )
        self.caaqms_chk_met.setChecked(True)
        unc_form.addRow("", self.caaqms_chk_met)

        root.addWidget(unc_box)

        # ── summary log ───────────────────────────────────────────────────────
        log_box = QGroupBox("4 · Validation Summary (populated after Run Pipeline)")
        log_layout = QVBoxLayout(log_box)

        self.caaqms_log = QTextEdit()
        self.caaqms_log.setReadOnly(True)
        self.caaqms_log.setPlaceholderText(
            "Uncertainty statistics will appear here after you run the pipeline …\n\n"
            "Expected outputs per station:\n"
            "  • RMSE (µg/m³ or ppb) per pollutant\n"
            "  • Mean bias vs satellite CH₄ / XCO₂ composite\n"
            "  • Pearson r (monthly means, n = 60 months)\n"
            "  • 95% confidence interval on hotspot classification\n"
            "  • Wind-weighted footprint radius estimate"
        )
        mono = QFont("Courier New", 9)
        self.caaqms_log.setFont(mono)
        self.caaqms_log.setMinimumHeight(140)
        log_layout.addWidget(self.caaqms_log)

        btn_row = QHBoxLayout()
        btn_clear_log = QPushButton("Clear log")
        btn_clear_log.clicked.connect(self.caaqms_log.clear)
        btn_export_log = QPushButton("Export summary CSV …")

        def export_caaqms_log():
            path, _ = QFileDialog.getSaveFileName(
                widget, "Save CAAQMS Summary", "caaqms_uncertainty_summary.csv",
                "CSV (*.csv)"
            )
            if path:
                with open(path, "w", encoding="utf-8") as f:
                    f.write(self.caaqms_log.toPlainText())
                QMessageBox.information(widget, "Exported", f"Saved to:\n{path}")

        btn_export_log.clicked.connect(export_caaqms_log)
        btn_row.addWidget(btn_clear_log)
        btn_row.addWidget(btn_export_log)
        btn_row.addStretch()
        log_layout.addLayout(btn_row)
        root.addWidget(log_box)

        root.addStretch()

        # ── internal: load CSV and populate preview ────────────────────────────
        def _load_caaqms_preview(path):
            try:
                with open(path, newline="", encoding="utf-8-sig") as f:
                    reader = csv.DictReader(f)
                    rows = list(reader)

                if not rows:
                    QMessageBox.warning(widget, "Empty file", "The CSV appears to be empty.")
                    return

                # summary stats
                stations = set()
                dates = []
                for r in rows:
                    s = r.get("Station") or r.get("station") or r.get("STATION") or "?"
                    stations.add(s)
                    d = r.get("Date") or r.get("date") or r.get("DATE") or ""
                    if d:
                        dates.append(d)

                self.caaqms_stations_lbl.setText(
                    f"{len(stations)} station(s): {', '.join(sorted(stations)[:6])}"
                    + (" …" if len(stations) > 6 else "")
                )
                self.caaqms_rows_lbl.setText(f"{len(rows):,} records")
                if dates:
                    self.caaqms_date_range_lbl.setText(f"{min(dates)}  →  {max(dates)}")

                # detect which of our expected columns are present
                sample_keys = list(rows[0].keys())
                present = [c for c in ALL_COLS if c in sample_keys]
                missing = [c for c in ALL_COLS if c not in sample_keys]

                if missing:
                    self.caaqms_log.append(
                        f"⚠ Columns not found in CSV (will be skipped): {', '.join(missing)}\n"
                        f"  Columns detected: {', '.join(sample_keys)}"
                    )

                # populate preview table
                self.caaqms_preview.setColumnCount(len(present))
                self.caaqms_preview.setHorizontalHeaderLabels(present)
                preview_rows = rows[:100]
                self.caaqms_preview.setRowCount(len(preview_rows))

                for ri, row in enumerate(preview_rows):
                    for ci, col in enumerate(present):
                        val = row.get(col, "")
                        item = QTableWidgetItem(str(val))
                        # colour-code missing / zero values
                        if val in ("", "NA", "NaN", "null", "NULL", "--"):
                            item.setBackground(QColor(80, 40, 40))
                        self.caaqms_preview.setItem(ri, ci, item)

                self.caaqms_log.append(
                    f"✓ Loaded {len(rows):,} rows · {len(stations)} station(s) · "
                    f"{len(present)}/{len(ALL_COLS)} expected columns present.\n"
                    f"  Ready for uncertainty analysis after pipeline run."
                )

            except Exception as e:
                QMessageBox.critical(widget, "Load error", f"Could not read CSV:\n{e}")

        # store reference so pipeline runner can access data
        self._caaqms_load_fn = _load_caaqms_preview

        return widget
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

        self.btn_load_ch4       = self._result_btn("🗺  Load CH₄ composite — TROPOMI/Sentinel-5P (GeoTIFF)", self._load_ch4)
        self.btn_load_co2       = self._result_btn("🗺  Load XCO₂ composite — OCO-2/3 + GOSAT ACOS (GeoTIFF)", self._load_co2)
        self.btn_load_gosat_ch4 = self._result_btn("🛰  Load XCH₄ composite — GOSAT NIES (GeoTIFF)", self._load_gosat_ch4)
        self.btn_load_hotspot   = self._result_btn("📌  Load hotspot shapefile", self._load_hotspot)
        self.btn_load_soc       = self._result_btn("🟤  Load SOC point layer",   self._load_soc)
        self.btn_open_dir       = self._result_btn("📁  Open output folder in explorer", self._open_output_dir)

        for btn in [self.btn_load_ch4, self.btn_load_co2, self.btn_load_gosat_ch4,
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

    # ------------------------------------------------------------------
    # Persistent settings  (QSettings → Windows registry / INI file)
    # ------------------------------------------------------------------

    def _load_settings(self):
        """Restore saved credentials and paths on dialog open."""
        s = QSettings(self.SETTINGS_ORG, self.SETTINGS_APP)

        gee_project = s.value("gee/project", "")
        if gee_project:
            self.gee_project_edit.setText(gee_project)

        ed_user = s.value("earthdata/username", "")
        ed_pass = s.value("earthdata/password", "")
        if ed_user:
            self.earthdata_user_edit.setText(ed_user)
        if ed_pass:
            self.earthdata_pass_edit.setText(ed_pass)

        nies_user = s.value("nies/username", "")
        nies_pass = s.value("nies/password", "")
        if nies_user:
            self.nies_user_edit.setText(nies_user)
        if nies_pass:
            self.nies_pass_edit.setText(nies_pass)

        output_dir = s.value("paths/output_dir", "")
        if output_dir:
            self.output_dir_edit.setText(output_dir)

    def _save_settings(self):
        """Persist credentials and paths so they survive QGIS restarts."""
        s = QSettings(self.SETTINGS_ORG, self.SETTINGS_APP)

        s.setValue("gee/project",        self.gee_project_edit.text().strip())
        s.setValue("earthdata/username",  self.earthdata_user_edit.text().strip())
        s.setValue("earthdata/password",  self.earthdata_pass_edit.text())
        s.setValue("nies/username",       self.nies_user_edit.text().strip())
        s.setValue("nies/password",       self.nies_pass_edit.text())
        s.setValue("paths/output_dir",    self.output_dir_edit.text().strip())

    def _update_sat_availability(self):
        """Recount credentialled sources and update the status label."""
        has_earthdata = bool(self.earthdata_user_edit.text().strip()
                             and self.earthdata_pass_edit.text())
        has_nies = bool(self.nies_user_edit.text().strip()
                        and self.nies_pass_edit.text())
        available = []
        if self.chk_tropomi.isChecked():
            available.append("TROPOMI")
        if self.chk_oco2.isChecked() and has_earthdata:
            available.append("OCO-2")
        if self.chk_oco3.isChecked() and has_earthdata:
            available.append("OCO-3")
        if self.chk_gosat.isChecked() and has_earthdata:
            available.append("GOSAT XCO₂")
        if self.chk_gosat_ch4.isChecked() and has_nies:
            available.append("GOSAT XCH₄")

        n = len(available)
        if n >= 2:
            self.sat_avail_label.setText(
                f"<span style='color:#1e8449'>✅  {n} source(s) available: "
                f"{', '.join(available)} — pipeline can run.</span>"
            )
        else:
            missing = []
            if not has_earthdata:
                missing.append("add EarthData credentials for OCO/GOSAT XCO₂")
            if not has_nies:
                missing.append("add NIES credentials for GOSAT XCH₄")
            hint = "; or ".join(missing) if missing else "enable more sources"
            self.sat_avail_label.setText(
                f"<span style='color:#c0392b'>⚠  Only {n} source(s) available "
                f"({', '.join(available) or 'none'}). Need ≥2 — {hint}.</span>"
            )

    def _test_nies_connection(self):
        user = self.nies_user_edit.text().strip()
        pwd  = self.nies_pass_edit.text()
        if not user or not pwd:
            self.nies_status_lbl.setText("⚠ Enter username and password first.")
            self.nies_status_lbl.setStyleSheet("color: #d35400;")
            return
        self.nies_status_lbl.setText("Connecting…")
        self.nies_status_lbl.setStyleSheet("color: #5d6d7e;")
        try:
            import paramiko
            t = paramiko.Transport(("prdct.gosat-2.nies.go.jp", 22))
            t.connect(username=user, password=pwd)
            t.close()
            self.nies_status_lbl.setText("✅  Connected successfully")
            self.nies_status_lbl.setStyleSheet("color: #1e8449;")
            self._update_sat_availability()
        except ImportError:
            self.nies_status_lbl.setText("❌  paramiko not installed — run: pip install paramiko")
            self.nies_status_lbl.setStyleSheet("color: #c0392b;")
        except Exception as e:
            self.nies_status_lbl.setText(f"❌  {e}")
            self.nies_status_lbl.setStyleSheet("color: #c0392b;")

    def _load_earthdata_netrc(self):
        """Pre-fill EarthData credentials from ~/.netrc (same convention as the reference repo)."""
        import netrc, os
        netrc_path = os.path.join(os.path.expanduser("~"), ".netrc")
        try:
            creds = netrc.netrc(netrc_path).authenticators("urs.earthdata.nasa.gov")
            if creds:
                username, _, password = creds
                self.earthdata_user_edit.setText(username)
                self.earthdata_pass_edit.setText(password)
                self.netrc_status_lbl.setText("✅ Loaded from .netrc")
                self.netrc_status_lbl.setStyleSheet("color: #1e8449;")
            else:
                self.netrc_status_lbl.setText("⚠ No urs.earthdata.nasa.gov entry in .netrc")
                self.netrc_status_lbl.setStyleSheet("color: #d35400;")
        except FileNotFoundError:
            self.netrc_status_lbl.setText("⚠ ~/.netrc not found")
            self.netrc_status_lbl.setStyleSheet("color: #d35400;")
        except Exception as e:
            self.netrc_status_lbl.setText(f"⚠ {e}")
            self.netrc_status_lbl.setStyleSheet("color: #c0392b;")

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
            "gee_project":      self.gee_project_edit.text().strip(),
            "earthdata_user":   self.earthdata_user_edit.text().strip() or None,
            "earthdata_pass":   self.earthdata_pass_edit.text() or None,
            "nies_user":        self.nies_user_edit.text().strip() or None,
            "nies_pass":        self.nies_pass_edit.text() or None,
            "satellites": {
                "tropomi":    self.chk_tropomi.isChecked(),
                "oco2":       self.chk_oco2.isChecked(),
                "oco3":       self.chk_oco3.isChecked(),
                "gosat":      self.chk_gosat.isChecked(),
                "gosat_ch4":  self.chk_gosat_ch4.isChecked(),
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
        self._save_settings()      # persist before pipeline starts
        config = self._build_config()

        # Validate
        errors = []
        if not config["gee_project"]:
            errors.append("GEE Project ID is required (Setup tab).")
        if not config["output_dir"]:
            errors.append("Output directory is required (Setup tab).")

        # Count credentialled + checked sources
        sats = config["satellites"]
        has_earthdata = bool(config.get("earthdata_user") and config.get("earthdata_pass"))
        has_nies      = bool(config.get("nies_user") and config.get("nies_pass"))
        available_sources = []
        if sats.get("tropomi"):
            available_sources.append("TROPOMI/Sentinel-5P")
        if sats.get("oco2") and has_earthdata:
            available_sources.append("OCO-2")
        if sats.get("oco3") and has_earthdata:
            available_sources.append("OCO-3")
        if sats.get("gosat") and has_earthdata:
            available_sources.append("GOSAT XCO₂")
        if sats.get("gosat_ch4") and has_nies:
            available_sources.append("GOSAT XCH₄")

        if len(available_sources) < 2:
            errors.append(
                f"At least 2 data sources with valid credentials are required to run.\n"
                f"  Currently available: {', '.join(available_sources) or 'none'}\n"
                f"  • TROPOMI needs only GEE project (always available if GEE is set)\n"
                f"  • OCO-2 / OCO-3 / GOSAT XCO₂ need NASA EarthData credentials\n"
                f"  • GOSAT XCH₄ needs NIES credentials"
            )

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
        self.tabs.setCurrentIndex(3)  # show Run tab

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
            self.tabs.setCurrentIndex(4)  # jump to Results tab
            QMessageBox.information(
                self, "Done",
                "Pipeline finished successfully.\n\n"
                "Switch to the Results tab to load layers into QGIS."
            )
        else:
            self.log("\n❌  Pipeline failed — see log above for details.")
            QMessageBox.critical(self, "Pipeline failed",
                                 "An error occurred. Check the pipeline log for details.")

    def closeEvent(self, event):
        self._save_settings()
        super().closeEvent(event)

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
        self._load_raster("ch4_composite.tif", "CH₄ Composite (TROPOMI/Sentinel-5P)")

    def _load_co2(self):
        self._load_raster("xco2_composite.tif", "XCO₂ Composite (OCO-2/3 + GOSAT ACOS)")

    def _load_gosat_ch4(self):
        self._load_raster("gosat_ch4_composite.tif", "XCH₄ Composite (GOSAT NIES)")

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
