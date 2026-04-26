"""
Minimal stubs for Kodi Python API modules.

Patches sys.modules so cacheman can be imported and tested outside Kodi.
Import this module BEFORE importing anything from cacheman.
"""

import os
import sys
import tempfile
from unittest.mock import MagicMock

# ---------------------------------------------------------------------------
# Shared temp directory — used by translatePath to simulate Kodi's VFS
# ---------------------------------------------------------------------------

TEMP_DIR = tempfile.mkdtemp(prefix='cacheman_test_')


# ---------------------------------------------------------------------------
# xbmc — silence all log calls, expose log level constants
# ---------------------------------------------------------------------------

class _XbmcStub:
    LOGDEBUG   = 0
    LOGINFO    = 1
    LOGWARNING = 2
    LOGERROR   = 4

    def log(self, msg, level=None):
        pass  # suppress output during tests


sys.modules['xbmc'] = _XbmcStub()


# ---------------------------------------------------------------------------
# xbmcaddon — Addon() raises RuntimeError so ConnectionManager falls back
# to the special://temp/ path (which we map to TEMP_DIR below)
# ---------------------------------------------------------------------------

_xbmcaddon = MagicMock()
_xbmcaddon.Addon.side_effect = RuntimeError('no Kodi context in tests')
sys.modules['xbmcaddon'] = _xbmcaddon


# ---------------------------------------------------------------------------
# xbmcvfs — translatePath maps special://temp/ to our real TEMP_DIR
# ---------------------------------------------------------------------------

def _translate_path(path: str) -> str:
    """Map Kodi's virtual paths to real filesystem paths."""
    if path.startswith('special://temp/'):
        return os.path.join(TEMP_DIR, path[len('special://temp/'):])
    return path


_xbmcvfs = MagicMock()
_xbmcvfs.translatePath = _translate_path
sys.modules['xbmcvfs'] = _xbmcvfs
