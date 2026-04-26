from .constants   import DTYPE_JSON, DTYPE_STR, DTYPE_BYTES
from .connection  import ConnectionManager
from .schema      import SchemaManager
from .serializer  import Serializer
from .store       import CacheStore
from .decorators  import CacheDecorators

class CacheManager:
    """
    Public facade for the CacheMan library.

    Wires together all components and exposes a single, clean API.
    Follows the Facade pattern — callers interact only with this class;
    they have no knowledge of ConnectionManager, SchemaManager,
    Serializer, CacheStore, or CacheDecorators.

    Usage:
        from cacheman import CacheManager

        cache = CacheManager()

        cache.set("channels:123", data, ttl=3600)
        data = cache.get("channels:123")
        cache.delete("channels:123")

        cache.set("thumb:456", img_bytes, dtype='bytes', ttl=86400)
        cache.set("token:raw", "Bearer abc", dtype='str')

        @cache.cached(key="channels:{category_id}", ttl=3600)
        def fetch_channels(category_id):
            return api_call(category_id)

        @cache.cached_async(key="epg:{channel_id}", ttl=1800)
        async def fetch_epg(channel_id):
            return await async_api_call(channel_id)
    """

    def __init__(
        self,
        db_name:            str  = 'cache.db',
        max_entries:        int  = 10000,
        compress:           bool = False,
        compress_threshold: int  = 1024,
        compress_level:     int  = 6,
    ):
        """
        Args:
            db_name:            SQLite filename (stored in addon profile dir).
            max_entries:        Maximum keys before LRU eviction.
            compress:           Enable zlib compression for stored data.
            compress_threshold: Compress only when raw bytes exceed this size.
            compress_level:     zlib level 1–9 (default 6).
        """
        conn_manager = ConnectionManager(db_name)
        SchemaManager(conn_manager).initialize()

        serializer = Serializer(compress, compress_threshold, compress_level)

        self._store      = CacheStore(conn_manager, serializer, max_entries)
        self._decorators = CacheDecorators(self._store)
        self._conn       = conn_manager

                                                                            
                                                    
                                                                            

    def get(self, key: str):
        """Return the value for key, or None if missing / expired."""
        return self._store.get(key)

    def set(self, key: str, value, ttl: int = None, dtype: str = DTYPE_JSON):
        """
        Store value under key.

        Args:
            key:   Cache key string.
            value: Value to store.
            ttl:   Seconds until expiry. None = permanent.
            dtype: 'json' (default), 'str', or 'bytes'.
        """
        return self._store.set(key, value, ttl=ttl, dtype=dtype)

    def delete(self, key: str):
        """Delete a key. No-op if it doesn't exist."""
        return self._store.delete(key)

    def exists(self, key: str) -> bool:
        """Return True if key exists and has not expired."""
        return self._store.exists(key)

    def ttl(self, key: str) -> int:
        """
        Return remaining TTL in seconds.
        -1 = permanent, -2 = key not found.
        """
        return self._store.ttl(key)

    def expire(self, key: str, ttl: int) -> bool:
        """Set or update the TTL of an existing key."""
        return self._store.expire(key, ttl)

    def persist(self, key: str) -> bool:
        """Remove the expiry from a key, making it permanent."""
        return self._store.persist(key)

    def keys(self, pattern: str = '*') -> list:
        """Return all non-expired keys matching a glob pattern."""
        return self._store.keys(pattern)

    def clear(self, pattern: str = None):
        """Delete keys matching pattern, or flush all if no pattern."""
        return self._store.clear(pattern)

    def cleanup(self) -> int:
        """Delete all expired keys. Returns the number removed."""
        return self._store.cleanup()

    def info(self) -> dict:
        """Return a statistics snapshot of the cache."""
        return self._store.info()

    def flush(self):
        """Force-flush pending LRU updates to disk (normally automatic)."""
        return self._store.flush()

    def vacuum(self):
        """Reclaim disk space from deleted rows (SQLite VACUUM)."""
        return self._store.vacuum()

                                                                            
                                                   
                                                                            

    def cached(self, key: str = None, ttl: int = None, dtype: str = DTYPE_JSON):
        """
        Decorator for caching synchronous functions.

        Args:
            key:   Key template with {param} placeholders. None = auto-key.
            ttl:   Time-to-live in seconds. None = no expiry.
            dtype: 'json' (default), 'str', or 'bytes'.
        """
        return self._decorators.cached(key=key, ttl=ttl, dtype=dtype)

    def cached_async(self, key: str = None, ttl: int = None, dtype: str = DTYPE_JSON):
        """
        Decorator for caching asynchronous functions.

        Args:
            key:   Key template with {param} placeholders. None = auto-key.
            ttl:   Time-to-live in seconds. None = no expiry.
            dtype: 'json' (default), 'str', or 'bytes'.
        """
        return self._decorators.cached_async(key=key, ttl=ttl, dtype=dtype)

                                                                            
                   
                                                                            

    def close(self):
        """
        Flush pending LRU updates and close the database connection.
        Call this on addon shutdown to avoid losing buffered state.
        """
        self._store.flush()
        self._conn.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __del__(self):
        self.close()
