# CacheMan

A Redis-like SQLite cache module for Kodi addons.  
Explicit string keys, `set()` level TTLs, no magic function names.

## Features

- **Redis-like API**: `get`, `set`, `delete`, `exists`, `ttl`, `expire`, `persist`, `keys`, `clear`
- **SQLite-based**: Persistent across sessions, much more robust than flat JSON files.
- **Thread-safe**: Thread-local connections by default.
- **Async support**: Decorators available for both sync and async functions.
- **Compression**: Optional zlib compression for large payloads.
- **LRU eviction**: Configurable maximum key limits.
- **TTL per key**: No global config — each `set()` defines its own TTL.
- **Pattern matching**: `keys("channels:*")` and `clear("channels:*")` with GLOB pushdown.
- **Zero-config**: Works out of the box.

---

## Installation

### As a script module

1. Copy `script.module.cacheman/` to `~/.kodi/addons/`
2. Add to your `addon.xml`:

```xml
<requires>
    <import addon="script.module.cacheman" version="1.0.0"/>
</requires>
```

### Local Development

```bash
ln -s /path/to/script.module.cacheman ~/.kodi/addons/
```

---

## Basic Usage

```python
from cacheman import CacheManager

cache = CacheManager()

# SET with TTL
cache.set("channels:123", data, ttl=3600)

# GET
data = cache.get("channels:123")  # None if missing or expired

# DELETE
cache.delete("channels:123")

# EXISTS
if cache.exists("channels:123"):
    ...

# Time-to-live remaining
secs = cache.ttl("channels:123")  # -1 = permanent, -2 = missing

# Update TTL
cache.expire("channels:123", ttl=7200)

# Make permanent
cache.persist("channels:123")
```

### Context Manager (`with` statement)

Ideal for quick scripts or background tasks where you want to ensure the memory buffer flushes and the connection closes cleanly when done:

```python
with CacheManager() as cache:
    cache.set("epg:123", epg_data, ttl=3600)
    data = cache.get("epg:123")
# The cache automatically forces flush() and executes close() at the end
```

---

## Pattern operations

```python
# List keys (glob)
cache.keys("*")             # all keys
cache.keys("channels:*")    # only channels
cache.keys("*:epg:*")       # EPG for any channel

# Clear by pattern
cache.clear()               # total flush
cache.clear("channels:*")   # only channels
cache.clear("epg:456:*")    # EPG for channel 456
```

---

## Decorators (sync)

```python
cache = CacheManager()

# Explicit key with placeholders
@cache.cached(key="channels:{category_id}", ttl=3600)
def fetch_channels(category_id):
    return api_call(category_id)

# Automatic key (func_name:arg=val:...)
@cache.cached(ttl=1800)
def fetch_data(x, y):
    return compute(x, y)
# → key: "fetch_data:x=5:y=hello"

# No TTL (permanent)
@cache.cached(key="token:csrf")
def get_csrf():
    return fetch_token()
```

---

## Decorators (async)

```python
import aiohttp

cache = CacheManager()

@cache.cached_async(key="epg:{channel_id}", ttl=1800)
async def fetch_epg(channel_id):
    async with aiohttp.ClientSession() as session:
        async with session.get(f'https://api/epg/{channel_id}') as resp:
            return await resp.json()

# Usage
data = await fetch_epg(456)
```

---

## With Compression and Dtypes

```python
cache = CacheManager(
    compress=True,
    compress_threshold=1024,  # compress if > 1KB
)

# Defaults to JSON (dict/list)
cache.set("epg:huge", large_data, ttl=3600)  # Compressed automatically

# Store strings (stored directly as UTF-8 bytes, not as a JSON "string")
from cacheman import DTYPE_STR
cache.set("token:raw", "Bearer abc", dtype=DTYPE_STR)

# Store raw bytes
from cacheman import DTYPE_BYTES
cache.set("thumb:1", b'\xff\xd8...', dtype=DTYPE_BYTES)
```

---

## Full API Reference

### `__init__(...)`

```python
cache = CacheManager(
    db_name='cache.db',        # SQLite file (in addon profile dir)
    max_entries=10000,         # LRU eviction limit
    compress=False,            # Enable zlib compression
    compress_threshold=1024,   # Compress only if > N bytes
    compress_level=6           # Compression level (1-9)
)
```

### `get(key)`

Returns the value, or `None` if the key does not exist or has expired.

```python
data = cache.get("channels:123")
```

### `set(key, value, ttl=None, dtype='json')`

Stores the value. `ttl=None` → no expiration. Valid dtypes: `'json'`, `'str'`, `'bytes'`.

```python
from cacheman import DTYPE_JSON, DTYPE_STR, DTYPE_BYTES

cache.set("channels:123", data, ttl=3600)               # default JSON
cache.set("token:csrf", token, dtype=DTYPE_STR)         # raw string
cache.set("thumb", img_bytes, dtype=DTYPE_BYTES)        # raw binary
```

### `delete(key)`

Deletes a key. No-op if missing.

```python
cache.delete("channels:123")
```

### `exists(key) → bool`

```python
if cache.exists("channels:123"):
    ...
```

### `ttl(key) → int`

```
>= 0   seconds remaining
-1     key exists, no expiration (permanent)
-2     key does not exist
```

### `expire(key, ttl) → bool`

Updates the TTL of an existing key. Returns `True` if it existed.

### `persist(key) → bool`

Removes the TTL from a key (making it permanent). Returns `True` if it existed.

### `keys(pattern='*') → List[str]`

Lists active keys (not expired) matching the glob pattern.

```python
cache.keys("*")           # all
cache.keys("epg:*")       # prefix epg:
cache.keys("*:token")     # suffix :token
```

### `clear(pattern=None)`

```python
cache.clear()             # full flush
cache.clear("epg:*")      # only matches
```

### `cleanup() → int`

Actively deletes all expired keys at once. Returns the number of keys deleted.

```python
deleted = cache.cleanup()
```

### `info() → dict`

```python
stats = cache.info()
# {
#     'total_keys':       500,
#     'active_keys':      480,
#     'expired_keys':     20,
#     'total_size_bytes': 2097152,
#     'compressed_keys':  120,
#     'lru_buffered':     12,
#     'by_dtype':         {'json': 480, 'str': 20, 'bytes': 0},
#     'oldest_entry':     1714000000,
#     'newest_entry':     1714050000,
# }
```

### `cached(key=None, ttl=None, dtype='json')`

Sync Decorator. See Decorators section.

### `cached_async(key=None, ttl=None, dtype='json')`

Async Decorator. See Decorators section.

### `flush()`

Forces the LRU memory buffer to be saved to disk. (Runs automatically when reaching the threshold or on `close()`).

### `vacuum()`

Optimizes the DB (reclaims space from deleted keys).

### `close()`

Closes the connection. Call during addon shutdown.

---

## Full Example — Video Addon

```python
from cacheman import CacheManager
import requests

cache = CacheManager(
    db_name='myaddon_cache.db',
    compress=True,
    compress_threshold=2048
)

@cache.cached(key="channels", ttl=21600)      # 6 hours
def get_channels():
    return requests.get('https://api.example.com/channels').json()

@cache.cached(key="epg:{channel_id}", ttl=3600)
def get_epg(channel_id):
    return requests.get(f'https://api.example.com/epg/{channel_id}').json()

@cache.cached(key="token:csrf", ttl=86400)    # 24 hours
def get_csrf():
    return requests.post('https://api.example.com/auth').json()['token']
```

---

## Clear Cache from Kodi Settings

```xml
<!-- settings.xml -->
<setting label="Clear Cache" type="action"
         action="RunScript(special://home/addons/plugin.video.myaddon/clear_cache.py)"/>
```

```python
# clear_cache.py
from cacheman import CacheManager
import xbmcgui

cache = CacheManager(db_name='myaddon_cache.db')
cache.clear()

xbmcgui.Dialog().notification('My Addon', 'Cache cleared', xbmcgui.NOTIFICATION_INFO, 3000)
```

---

## Thread safety

```python
import threading
from cacheman import CacheManager

cache = CacheManager()

def worker(channel_id):
    # each thread gets its own SQLite connection
    cache.set(f"epg:{channel_id}", fetch(channel_id), ttl=3600)

threads = [threading.Thread(target=worker, args=(i,)) for i in range(10)]
for t in threads: t.start()
for t in threads: t.join()
```

---

## Design and Performance (Embedded SQLite)

CacheMan has been specifically optimized for the slow devices where Kodi typically runs (Raspberry Pi, Fire Stick, Android TV with eMMC/microSD):

- **WAL mode and cache_size**: Allows concurrent reads without blocking writes, reduces fsyncs.
- **Lazy LRU buffering**: Avoids writing to the DB on every single `get()`. It groups `last_accessed` timestamps in memory and performs batch flushes in a single async `executemany` operation.
- **Throttled Eviction**: LRU cleanup only runs every 20 `set()` calls, avoiding repetitive latency spikes during large loops.
- **SQLite GLOB pushdown**: Functions like `keys("prefix:*")` and `clear("prefix:*")` are resolved purely in SQLite's C-engine using the native disk index (avoids loading hundreds of MBs into RAM).

All these systems achieve latency `<1ms` comparable to local-network Redis, but keeping a zero-setup tolerance.

---


## License

MIT
