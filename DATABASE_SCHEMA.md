# CacheMan - Database Schema

## `cache` Table

```sql
CREATE TABLE IF NOT EXISTS cache (
    key           TEXT PRIMARY KEY,   
    data          BLOB NOT NULL,      
    compressed    INTEGER DEFAULT 0,  
    created_at    INTEGER NOT NULL,   
    expires_at    INTEGER,            
    last_accessed INTEGER NOT NULL,   
    data_size     INTEGER DEFAULT 0   
);

CREATE INDEX idx_expires_at    ON cache(expires_at);
CREATE INDEX idx_last_accessed ON cache(last_accessed);
```

---

## Columns

### `key` (TEXT PRIMARY KEY)

Explicit key provided by the addon code. There is no internal hashing — the key is exactly what is passed.

**Recommended Convention (namespace with `:`)**
```
channels:123
epg:456:HD
token:csrf
user:789:profile
```

### `data` (BLOB)

Serialized payload in bytes.

**Write Pipeline:**
```
Python Object
  → json.dumps()       → JSON string
  → .encode('utf-8')   → bytes
  → [zlib.compress()]  → compressed bytes (if compress=True and size > threshold)
  → stored in data column
```

**Read Pipeline:**
```
bytes from data column
  → [zlib.decompress()] → bytes (if compressed=1)
  → .decode('utf-8')    → JSON string
  → json.loads()        → Python Object
```

### `compressed` (INTEGER, 0 or 1)

Flag indicating if `data` is zlib compressed. Required to know if `zlib.decompress()` must be called upon read.

### `created_at` (INTEGER)

Unix timestamp when the entry was created. For informational purposes only (not used for TTL — that's `expires_at`).

### `expires_at` (INTEGER | NULL)

Absolute Unix timestamp of expiration.

- `NULL` → no expiration (permanent, like `PERSIST` in Redis)
- `value` → expires when `time.time() >= expires_at`

**Example:**
```python
cache.set("channels:1", data, ttl=3600)
# expires_at = int(time.time()) + 3600
# If now=1714000000 → expires_at=1714003600

cache.set("token:csrf", token)
# ttl=None → expires_at=NULL (permanent)
```

**Check during GET:**
```python
if row['expires_at'] is not None and time.time() >= row['expires_at']:
    # expired — delete and return None
```

### `last_accessed` (INTEGER)

Unix timestamp of the last time this key was read (updated on every successful `get()`).

Used for LRU eviction: when the total number of keys exceeds `max_entries`, those with the oldest `last_accessed` are deleted.

### `data_size` (INTEGER)

Size in bytes of the `data` column (post-compression if applicable). Used for metrics in `info()`.

---

## Real Row Examples

### Key without expiration, without compression

```sql
INSERT INTO cache VALUES (
    'token:csrf',                            
    X'7b22746f6b656e223a2261626331323334227d', 
    0,                                       
    1714000000,                              
    NULL,                                    
    1714000000,                              
    20                                       
);
```

### Key with TTL, with compression

```sql
INSERT INTO cache VALUES (
    'epg:456',                               
    X'789c...',                              
    1,                                       
    1714000000,                              
    1714003600,                              
    1714000000,                              
    8912                                     
);
```

---

## Indexes

### `idx_expires_at`
Speeds up expired cleanup and TTL checks:
```sql
DELETE FROM cache WHERE expires_at IS NOT NULL AND expires_at <= ?;
SELECT * FROM cache WHERE expires_at IS NULL OR expires_at > ?;
```

### `idx_last_accessed`
Speeds up LRU eviction:
```sql
DELETE FROM cache WHERE key IN (
    SELECT key FROM cache
    ORDER BY last_accessed ASC
    LIMIT ?
);
```

---

## Typical Sizes

| Addon Use Case | Keys   | Compression | DB Size   |
|----------------|--------|-------------|-----------|
| Simple IPTV    | ~500   | OFF         | ~2 MB     |
| Large EPG      | ~2000  | ON          | ~8 MB     |
| VOD+EPG        | ~5000  | ON          | ~15 MB    |


