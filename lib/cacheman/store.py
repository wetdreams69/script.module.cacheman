import fnmatch
import sqlite3
import threading
import time

import xbmc

from .connection  import ConnectionManager
from .serializer  import Serializer
from .constants   import DTYPE_JSON

class CacheStore:
    """
    Executes all cache read/write operations against SQLite.

    Responsibility: implement the Redis-like cache primitives (get, set,
    delete, ttl, expire, keys, clear, cleanup, info) and LRU eviction.
    It delegates connection management to ConnectionManager and
    data encoding to Serializer — it knows nothing about either.

    Performance design:
        - Lazy LRU:     last_accessed updates are buffered in memory and
                        flushed to disk every LRU_FLUSH_EVERY GETs (not per GET).
        - Throttled eviction: _enforce_limit() runs every EVICTION_CHECK_EVERY
                        SETs, not every single one.
        - GLOB pushdown: prefix patterns (e.g. 'channels:*') are evaluated by
                        SQLite directly, avoiding full table scan in Python.
    """

                                                          
    _LRU_FLUSH_EVERY       = 50

                                                
    _EVICTION_CHECK_EVERY  = 20

                                                                        
                                                     
    _EVICTION_TARGET       = 0.90

    def __init__(
        self,
        conn_manager: ConnectionManager,
        serializer:   Serializer,
        max_entries:  int = 10000,
    ):
        self._conn       = conn_manager
        self._serializer = serializer
        self.max_entries = max_entries

                                                                                    
        self._lru_lock   = threading.Lock()
        self._lru_buffer = {}                                      
        self._lru_dirty  = 0                              

                                           
        self._eviction_lock  = threading.Lock()
        self._set_count      = 0

                                                                            
                         
                                                                            

    def get(self, key: str):
        """
        Return the cached value for key, or None if missing / expired.

        TTL is checked lazily (expired entries deleted on access).
        last_accessed is buffered — not written to disk on every GET.
        """
        conn   = self._conn.get()
        cursor = conn.cursor()

        try:
            cursor.execute(
                'SELECT data, dtype, compressed, expires_at FROM cache WHERE key = ?',
                (key,),
            )
            row = cursor.fetchone()

            if not row:
                return None

                                     
            if row['expires_at'] is not None and time.time() >= row['expires_at']:
                cursor.execute('DELETE FROM cache WHERE key = ?', (key,))
                conn.commit()
                return None

                                                                   
            should_flush = self._touch_lru(key)
            if should_flush:
                self._flush_lru()

            dtype = row['dtype'] or DTYPE_JSON
            return self._serializer.deserialize(row['data'], bool(row['compressed']), dtype)

        except (sqlite3.Error, ValueError) as e:
            xbmc.log(f"[CacheMan] GET '{key}': {e}", xbmc.LOGERROR)
            return None

    def set(self, key: str, value, ttl: int = None, dtype: str = DTYPE_JSON):
        """
        Store value under key.

        Args:
            key:   Cache key string.
            value: Value to store — type must match dtype.
            ttl:   Seconds until expiry. None = permanent.
            dtype: 'json' (default), 'str', or 'bytes'.
        """
        conn   = self._conn.get()
        cursor = conn.cursor()

        try:
            now        = int(time.time())
            expires_at = (now + ttl) if ttl is not None else None

            data_bytes, is_compressed = self._serializer.serialize(value, dtype)

            cursor.execute(
                '''
                INSERT OR REPLACE INTO cache
                    (key, data, dtype, compressed,
                     created_at, expires_at, last_accessed, data_size)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''',
                (key, data_bytes, dtype, int(is_compressed),
                 now, expires_at, now, len(data_bytes)),
            )
            conn.commit()

                                                            
            with self._eviction_lock:
                self._set_count += 1
                should_check = (self._set_count % self._EVICTION_CHECK_EVERY == 0)

            if should_check:
                self._enforce_limit()

        except sqlite3.Error as e:
            xbmc.log(f"[CacheMan] SET '{key}': {e}", xbmc.LOGERROR)

    def delete(self, key: str):
        """Delete a key. No-op if the key doesn't exist."""
        conn = self._conn.get()
        try:
            conn.execute('DELETE FROM cache WHERE key = ?', (key,))
            conn.commit()
        except sqlite3.Error as e:
            xbmc.log(f"[CacheMan] DELETE '{key}': {e}", xbmc.LOGERROR)

    def exists(self, key: str) -> bool:
        """Return True if key exists and has not expired."""
        return self.get(key) is not None

                                                                            
                        
                                                                            

    def ttl(self, key: str) -> int:
        """
        Return remaining TTL in seconds.

        Returns:
             >= 0  — seconds remaining
            -1     — key exists, no expiry (permanent)
            -2     — key does not exist
        """
        conn   = self._conn.get()
        cursor = conn.cursor()

        try:
            cursor.execute('SELECT expires_at FROM cache WHERE key = ?', (key,))
            row = cursor.fetchone()

            if not row:
                return -2

            if row['expires_at'] is None:
                return -1

            return max(0, int(row['expires_at'] - time.time()))

        except sqlite3.Error as e:
            xbmc.log(f"[CacheMan] TTL '{key}': {e}", xbmc.LOGERROR)
            return -2

    def expire(self, key: str, ttl: int) -> bool:
        """
        Set or update the TTL of an existing key.

        Returns:
            True if the key existed and was updated, False otherwise.
        """
        conn   = self._conn.get()
        cursor = conn.cursor()

        try:
            expires_at = int(time.time()) + ttl
            cursor.execute(
                'UPDATE cache SET expires_at = ? WHERE key = ?',
                (expires_at, key),
            )
            conn.commit()
            return cursor.rowcount > 0

        except sqlite3.Error as e:
            xbmc.log(f"[CacheMan] EXPIRE '{key}': {e}", xbmc.LOGERROR)
            return False

    def persist(self, key: str) -> bool:
        """
        Remove the expiry from a key, making it permanent.

        Returns:
            True if the key existed and was updated, False otherwise.
        """
        conn   = self._conn.get()
        cursor = conn.cursor()

        try:
            cursor.execute(
                'UPDATE cache SET expires_at = NULL WHERE key = ?',
                (key,),
            )
            conn.commit()
            return cursor.rowcount > 0

        except sqlite3.Error as e:
            xbmc.log(f"[CacheMan] PERSIST '{key}': {e}", xbmc.LOGERROR)
            return False

                                                                            
                            
                                                                            

    def keys(self, pattern: str = '*') -> list:
        """
        Return all non-expired keys matching a glob pattern.

        Optimization: simple prefix patterns (e.g. 'channels:*') are
        pushed down to SQLite GLOB, avoiding a full table scan in Python.
        Complex patterns fall back to Python fnmatch.

        Args:
            pattern: Glob e.g. 'channels:*', '*:epg:*', '*'.

        Returns:
            List[str]
        """
        conn   = self._conn.get()
        cursor = conn.cursor()

        try:
            now = time.time()

                                                           
            if pattern == '*':
                cursor.execute(
                    'SELECT key FROM cache WHERE (expires_at IS NULL OR expires_at > ?)',
                    (now,),
                )
                return [row['key'] for row in cursor.fetchall()]

                                                                        
                                                                             
            if (pattern.endswith('*')
                    and '*' not in pattern[:-1]
                    and '?' not in pattern
                    and '[' not in pattern):
                cursor.execute(
                    '''
                    SELECT key FROM cache
                    WHERE key GLOB ?
                      AND (expires_at IS NULL OR expires_at > ?)
                    ''',
                    (pattern, now),
                )
                return [row['key'] for row in cursor.fetchall()]

                                                                                     
            cursor.execute(
                'SELECT key FROM cache WHERE (expires_at IS NULL OR expires_at > ?)',
                (now,),
            )
            return fnmatch.filter([row['key'] for row in cursor.fetchall()], pattern)

        except sqlite3.Error as e:
            xbmc.log(f"[CacheMan] KEYS '{pattern}': {e}", xbmc.LOGERROR)
            return []

    def clear(self, pattern: str = None):
        """
        Delete all keys matching pattern, or flush everything if no pattern.

        Args:
            pattern: Glob e.g. 'channels:*'. None = flush all.
        """
        conn   = self._conn.get()
        cursor = conn.cursor()

        try:
            if pattern is None:
                cursor.execute('DELETE FROM cache')
                xbmc.log("[CacheMan] Flushed entire cache", xbmc.LOGDEBUG)
            else:
                matching = self.keys(pattern)
                if matching:
                    placeholders = ','.join('?' * len(matching))
                    cursor.execute(
                        f'DELETE FROM cache WHERE key IN ({placeholders})',
                        matching,
                    )
                    xbmc.log(
                        f"[CacheMan] Deleted {len(matching)} keys matching '{pattern}'",
                        xbmc.LOGDEBUG,
                    )
            conn.commit()

        except sqlite3.Error as e:
            xbmc.log(f"[CacheMan] CLEAR '{pattern}': {e}", xbmc.LOGERROR)

                                                                            
                      
                                                                            

    def cleanup(self) -> int:
        """
        Delete all expired keys in one pass (active expiry sweep).

        Returns:
            Number of keys deleted.
        """
        conn   = self._conn.get()
        cursor = conn.cursor()

        try:
            now = time.time()
            cursor.execute(
                'DELETE FROM cache WHERE expires_at IS NOT NULL AND expires_at <= ?',
                (now,),
            )
            deleted = cursor.rowcount
            conn.commit()

            if deleted:
                xbmc.log(f"[CacheMan] cleanup: removed {deleted} expired keys", xbmc.LOGDEBUG)

            return deleted

        except sqlite3.Error as e:
            xbmc.log(f"[CacheMan] CLEANUP error: {e}", xbmc.LOGERROR)
            return 0

                                                                            
                    
                                                                            

    def info(self) -> dict:
        """
        Return a statistics snapshot of the cache.

        Returns:
            dict: {
                'total_keys':       int,
                'active_keys':      int,
                'expired_keys':     int,   -- not yet swept
                'total_size_bytes': int,
                'compressed_keys':  int,
                'by_dtype':         {'json': int, 'str': int, 'bytes': int},
                'lru_buffered':     int,   -- updates pending flush to disk
                'oldest_entry':     int or None,
                'newest_entry':     int or None,
            }
        """
        conn   = self._conn.get()
        cursor = conn.cursor()

        try:
            now = time.time()

            cursor.execute(
                '''
                SELECT
                    COUNT(*)                                                                     AS total,
                    SUM(CASE WHEN expires_at IS NULL OR expires_at > ?   THEN 1 ELSE 0 END)     AS active,
                    SUM(CASE WHEN expires_at IS NOT NULL AND expires_at <= ? THEN 1 ELSE 0 END) AS expired,
                    SUM(data_size)                                                               AS total_size,
                    SUM(CASE WHEN compressed = 1 THEN 1 ELSE 0 END)                             AS compressed,
                    MIN(created_at)                                                              AS oldest,
                    MAX(created_at)                                                              AS newest
                FROM cache
                ''',
                (now, now),
            )
            row = cursor.fetchone()

            cursor.execute('SELECT dtype, COUNT(*) AS n FROM cache GROUP BY dtype')
            by_dtype = {r['dtype']: r['n'] for r in cursor.fetchall()}

            with self._lru_lock:
                lru_buffered = self._lru_dirty

            return {
                'total_keys':       row['total']      or 0,
                'active_keys':      row['active']     or 0,
                'expired_keys':     row['expired']    or 0,
                'total_size_bytes': row['total_size'] or 0,
                'compressed_keys':  row['compressed'] or 0,
                'by_dtype':         by_dtype,
                'lru_buffered':     lru_buffered,
                'oldest_entry':     row['oldest'],
                'newest_entry':     row['newest'],
            }

        except sqlite3.Error as e:
            xbmc.log(f"[CacheMan] INFO error: {e}", xbmc.LOGERROR)
            return None

                                                                            
                     
                                                                            

    def flush(self):
        """
        Force-flush any pending LRU updates to disk.
        Call this before closing the connection or on addon shutdown.
        """
        self._flush_lru()

    def vacuum(self):
        """Reclaim disk space from deleted rows (SQLite VACUUM)."""
        self._flush_lru()                                                   
        conn = self._conn.get()
        try:
            conn.execute('VACUUM')
            xbmc.log("[CacheMan] Database vacuumed", xbmc.LOGDEBUG)
        except sqlite3.Error as e:
            xbmc.log(f"[CacheMan] VACUUM error: {e}", xbmc.LOGERROR)

                                                                            
                                    
                                                                            

    def _touch_lru(self, key: str) -> bool:
        """
        Record a key access in the in-memory LRU buffer.

        Returns:
            True if the buffer has reached the flush threshold.
        """
        with self._lru_lock:
            self._lru_buffer[key] = int(time.time())
            self._lru_dirty += 1
            return self._lru_dirty >= self._LRU_FLUSH_EVERY

    def _flush_lru(self):
        """
        Write buffered last_accessed updates to disk in a single batch.

        Uses executemany() — one transaction for all pending updates
        instead of one commit per GET.
        """
        with self._lru_lock:
            if not self._lru_buffer:
                return
            snapshot         = list(self._lru_buffer.items())
            self._lru_buffer = {}
            self._lru_dirty  = 0

        try:
            conn = self._conn.get()
            conn.executemany(
                'UPDATE cache SET last_accessed = ? WHERE key = ?',
                [(ts, k) for k, ts in snapshot],
            )
            conn.commit()
            xbmc.log(f"[CacheMan] LRU flush: {len(snapshot)} keys", xbmc.LOGDEBUG)

        except sqlite3.Error as e:
            xbmc.log(f"[CacheMan] LRU flush error: {e}", xbmc.LOGERROR)

                                                                            
                                 
                                                                            

    def _enforce_limit(self):
        """
        Evict LRU keys when total exceeds max_entries.

        Evicts down to _EVICTION_TARGET × max_entries to avoid running
        again on every subsequent SET near the limit.
        """
        conn   = self._conn.get()
        cursor = conn.cursor()

        try:
            cursor.execute('SELECT COUNT(*) AS n FROM cache')
            count = cursor.fetchone()['n']

            if count <= self.max_entries:
                return

                                                                    
            target = int(self.max_entries * self._EVICTION_TARGET)
            evict  = count - target

            cursor.execute(
                '''
                DELETE FROM cache WHERE key IN (
                    SELECT key FROM cache
                    ORDER BY last_accessed ASC
                    LIMIT ?
                )
                ''',
                (evict,),
            )
            conn.commit()
            xbmc.log(
                f"[CacheMan] Evicted {evict} LRU keys (target: {target})",
                xbmc.LOGDEBUG,
            )

        except sqlite3.Error as e:
            xbmc.log(f"[CacheMan] LRU eviction error: {e}", xbmc.LOGERROR)
