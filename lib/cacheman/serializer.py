import json
import zlib

from .constants import DTYPE_JSON, DTYPE_STR, DTYPE_BYTES

class Serializer:
    """
    Converts Python values to/from raw bytes for SQLite storage.

    Responsibility: handle three data types (json, str, bytes) and
    optional zlib compression. It has no knowledge of the database,
    keys, or TTL logic.

    Supported dtypes:
        'json'  — any JSON-serializable Python object  (default)
        'str'   — plain string                         → stored as UTF-8
        'bytes' — raw binary                           → stored as-is
    """

    VALID_DTYPES = frozenset({DTYPE_JSON, DTYPE_STR, DTYPE_BYTES})

    def __init__(
        self,
        compress: bool  = False,
        compress_threshold: int = 1024,
        compress_level: int     = 6,
    ):
        """
        Args:
            compress:           Enable zlib compression.
            compress_threshold: Compress only when raw bytes exceed this size.
            compress_level:     zlib level 1–9 (default 6).
        """
        self.compress           = compress
        self.compress_threshold = compress_threshold
        self.compress_level     = compress_level

                                                                            
                    
                                                                            

    def serialize(self, data, dtype: str) -> tuple:
        """
        Encode a Python value to bytes, compressing if configured.

        Args:
            data:  The value to store.
            dtype: One of 'json', 'str', 'bytes'.

        Returns:
            tuple[bytes, bool]: (raw_bytes, is_compressed)

        Raises:
            TypeError:  dtype is invalid, or data doesn't match the dtype.
            ValueError: data is not JSON-serializable (dtype='json' only).
        """
        self._validate_dtype(dtype)

        raw = self._encode(data, dtype)

        if self.compress and len(raw) > self.compress_threshold:
            return zlib.compress(raw, level=self.compress_level), True

        return raw, False

    def deserialize(self, data_bytes, is_compressed: bool, dtype: str):
        """
        Decode bytes back to the original Python value.

        Args:
            data_bytes:     Raw bytes from the database.
            is_compressed:  Whether the bytes are zlib-compressed.
            dtype:          One of 'json', 'str', 'bytes'.

        Returns:
            The original Python value (dict/list/str/bytes/…).

        Raises:
            ValueError: on decompression or decoding failure.
            TypeError: if dtype is invalid.
        """
        self._validate_dtype(dtype)
        try:
            raw = zlib.decompress(data_bytes) if is_compressed else bytes(data_bytes)
            return self._decode(raw, dtype)
        except (zlib.error, UnicodeDecodeError, json.JSONDecodeError) as e:
            raise ValueError(f"Deserialization failed (dtype={dtype}): {e}")

                                                                            
                                       
                                                                            

    def _encode(self, data, dtype: str) -> bytes:
        if dtype == DTYPE_JSON:
            try:
                return json.dumps(data, default=str).encode('utf-8')
            except (TypeError, ValueError) as e:
                raise ValueError(f"Data is not JSON-serializable: {e}")

        if dtype == DTYPE_STR:
            if not isinstance(data, str):
                raise TypeError(
                    f"dtype='str' expects a str value, got {type(data).__name__}"
                )
            return data.encode('utf-8')

                             
        if not isinstance(data, (bytes, bytearray)):
            raise TypeError(
                f"dtype='bytes' expects bytes/bytearray, got {type(data).__name__}"
            )
        return bytes(data)

    def _decode(self, raw: bytes, dtype: str):
        if dtype == DTYPE_JSON:
            return json.loads(raw.decode('utf-8'))

        if dtype == DTYPE_STR:
            return raw.decode('utf-8')

        return raw                 

    def _validate_dtype(self, dtype: str):
        if dtype not in self.VALID_DTYPES:
            raise TypeError(
                f"Invalid dtype '{dtype}'. "
                f"Valid options: {', '.join(sorted(self.VALID_DTYPES))}"
            )
