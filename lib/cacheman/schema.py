import sqlite3

import xbmc

from .connection import ConnectionManager

class SchemaManager:
    """
    Manages the SQLite schema lifecycle: table creation and migrations.

    Responsibility: ensure the database structure is correct before any
    read/write operation. It has no knowledge of serialization or cache logic.
    """

    def __init__(self, conn_manager: ConnectionManager):
        self._conn = conn_manager

    def initialize(self):
        """
        Create the cache table and indexes if they don't exist,
        then apply any pending migrations.
        """
        conn   = self._conn.get()
        cursor = conn.cursor()

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS cache (
                key           TEXT PRIMARY KEY,
                data          BLOB    NOT NULL,
                dtype         TEXT    DEFAULT 'json',
                compressed    INTEGER DEFAULT 0,
                created_at    INTEGER NOT NULL,
                expires_at    INTEGER,
                last_accessed INTEGER NOT NULL,
                data_size     INTEGER DEFAULT 0
            )
        ''')

        self._migrate(cursor)

                                             
                                                                                     
        conn.execute("PRAGMA journal_mode=WAL")
                                                                                    
        conn.execute("PRAGMA synchronous=NORMAL")
                                                                                   
        conn.execute("PRAGMA cache_size=-8000")

        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_expires_at
            ON cache(expires_at)
        ''')

        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_last_accessed
            ON cache(last_accessed)
        ''')

        conn.commit()

                                                                            
                  
                                                                            

    def _migrate(self, cursor):
        """Apply incremental schema migrations (additive only)."""
        cursor.execute("PRAGMA table_info(cache)")
        columns = {row['name'] for row in cursor.fetchall()}

        if 'dtype' not in columns:
            cursor.execute("ALTER TABLE cache ADD COLUMN dtype TEXT DEFAULT 'json'")
            xbmc.log("[CacheMan] Migration applied: added 'dtype' column", xbmc.LOGDEBUG)
