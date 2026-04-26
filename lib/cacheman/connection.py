import os
import sqlite3
import threading

import xbmc
from xbmcvfs import translatePath

class ConnectionManager:
    """
    Manages thread-local SQLite connections and path resolution.

    Responsibility: know *where* the database lives and *how* to open
    a connection to it — one per thread, created on demand.

    It has no knowledge of the schema, serialization, or cache logic.
    """

    def __init__(self, db_name='cache.db'):
        """
        Args:
            db_name: SQLite filename. Stored inside the Kodi addon profile dir.
        """
        self.db_name = db_name
        self._local  = threading.local()

                                                                            
                    
                                                                            

    def get(self) -> sqlite3.Connection:
        """
        Return the thread-local SQLite connection, creating it if needed.

        Returns:
            sqlite3.Connection with row_factory set to sqlite3.Row.
        """
        if not hasattr(self._local, 'conn') or self._local.conn is None:
            self._local.conn = sqlite3.connect(
                self._resolve_path(),
                check_same_thread=False,
                timeout=10.0,
            )
            self._local.conn.row_factory = sqlite3.Row

        return self._local.conn

    def close(self):
        """Close the thread-local connection (call on addon shutdown)."""
        if hasattr(self._local, 'conn') and self._local.conn:
            try:
                self._local.conn.close()
                self._local.conn = None
            except sqlite3.Error as e:
                xbmc.log(f"[CacheMan] ConnectionManager.close: {e}", xbmc.LOGERROR)

                                                                            
                  
                                                                            

    def _resolve_path(self) -> str:
        """
        Resolve the full filesystem path for the SQLite file.

        Falls back to a temp directory when called outside a Kodi context
        (e.g. unit tests).
        """
        try:
            import xbmcaddon
            addon       = xbmcaddon.Addon()
            profile_dir = translatePath(addon.getAddonInfo('profile'))
        except RuntimeError:
            profile_dir = translatePath('special://temp/cacheman/')

        os.makedirs(profile_dir, exist_ok=True)
        return os.path.join(profile_dir, self.db_name)
