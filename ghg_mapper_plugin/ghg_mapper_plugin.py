"""
ghg_mapper_plugin.py
Main QGIS plugin class for GHG Mapper — Agricultural India.
Registers toolbar icon, menu entry, and opens the main dialog.
"""
import os
from qgis.core import Qgis, QgsProject
from qgis.gui import QgisInterface
from qgis.PyQt.QtWidgets import QAction
from qgis.PyQt.QtGui import QIcon


class GHGMapperPlugin:
    def __init__(self, iface: QgisInterface):
        self.iface = iface
        self.plugin_dir = os.path.dirname(__file__)
        self.actions = []
        self.menu = "GHG Mapper"
        self.dialog = None

    # ------------------------------------------------------------------
    # QGIS lifecycle
    # ------------------------------------------------------------------

    def initGui(self):
        icon_path = os.path.join(self.plugin_dir, "icon.png")
        self._add_action(
            icon_path,
            text="Open GHG Mapper",
            callback=self.open_main_dialog,
            tooltip="Multi-satellite GHG hotspot mapping with SOC/SIC integration",
        )

    def unload(self):
        for action in self.actions:
            self.iface.removePluginRasterMenu(self.menu, action)
            self.iface.removeToolBarIcon(action)
        self.actions.clear()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _add_action(self, icon_path, text, callback, tooltip=""):
        icon = QIcon(icon_path) if os.path.exists(icon_path) else QIcon()
        action = QAction(icon, text, self.iface.mainWindow())
        action.setToolTip(tooltip)
        action.triggered.connect(callback)
        self.iface.addToolBarIcon(action)
        self.iface.addPluginToRasterMenu(self.menu, action)
        self.actions.append(action)
        return action

    # ------------------------------------------------------------------
    # Entry point
    # ------------------------------------------------------------------

    def open_main_dialog(self):
        """Open (or re-raise) the main GHG Mapper dialog."""
        if self.dialog is None:
            from .ghg_mapper_dialog import GHGMapperDialog
            self.dialog = GHGMapperDialog(iface=self.iface, parent=self.iface.mainWindow())
        self.dialog.show()
        self.dialog.raise_()
        self.dialog.activateWindow()
