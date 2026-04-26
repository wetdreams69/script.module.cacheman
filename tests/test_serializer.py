"""
Unit tests for Serializer.

No Kodi APIs used — pure Python. No mocking or temp files needed.
"""

import sys
import os
import unittest
import zlib

# --- Kodi stubs must come first ---
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'lib'))
import tests.kodi_stubs  # noqa: F401 — patches sys.modules

from cacheman.serializer import Serializer
from cacheman.constants  import DTYPE_JSON, DTYPE_STR, DTYPE_BYTES


class TestSerializerJson(unittest.TestCase):
    """Serializer with dtype='json' (default)."""

    def setUp(self):
        self.s = Serializer()

    def _roundtrip(self, value):
        raw, compressed = self.s.serialize(value, DTYPE_JSON)
        result = self.s.deserialize(raw, compressed, DTYPE_JSON)
        return result

    def test_dict(self):
        data = {'key': 'value', 'num': 42, 'nested': {'a': [1, 2, 3]}}
        self.assertEqual(self._roundtrip(data), data)

    def test_list(self):
        data = [1, 'two', {'three': 3}, None, True]
        self.assertEqual(self._roundtrip(data), data)

    def test_integer(self):
        self.assertEqual(self._roundtrip(99), 99)

    def test_boolean(self):
        self.assertTrue(self._roundtrip(True))
        self.assertFalse(self._roundtrip(False))

    def test_none(self):
        self.assertIsNone(self._roundtrip(None))

    def test_empty_dict(self):
        self.assertEqual(self._roundtrip({}), {})

    def test_empty_list(self):
        self.assertEqual(self._roundtrip([]), [])

    def test_not_compressed_by_default(self):
        _, is_compressed = self.s.serialize({'x': 1}, DTYPE_JSON)
        self.assertFalse(is_compressed)

    def test_non_serializable_raises(self):
        class BadObj:
            def __str__(self):
                raise ValueError("Nope")
        with self.assertRaises(ValueError):
            self.s.serialize(BadObj(), DTYPE_JSON)


class TestSerializerStr(unittest.TestCase):
    """Serializer with dtype='str'."""

    def setUp(self):
        self.s = Serializer()

    def _roundtrip(self, value):
        raw, compressed = self.s.serialize(value, DTYPE_STR)
        return self.s.deserialize(raw, compressed, DTYPE_STR)

    def test_simple_string(self):
        self.assertEqual(self._roundtrip('hello world'), 'hello world')

    def test_empty_string(self):
        self.assertEqual(self._roundtrip(''), '')

    def test_unicode(self):
        value = 'canción: ñoño 🎵'
        self.assertEqual(self._roundtrip(value), value)

    def test_returns_str_type(self):
        result = self._roundtrip('test')
        self.assertIsInstance(result, str)

    def test_wrong_type_raises(self):
        with self.assertRaises(TypeError):
            self.s.serialize({'not': 'a string'}, DTYPE_STR)

    def test_int_raises(self):
        with self.assertRaises(TypeError):
            self.s.serialize(123, DTYPE_STR)


class TestSerializerBytes(unittest.TestCase):
    """Serializer with dtype='bytes'."""

    def setUp(self):
        self.s = Serializer()

    def _roundtrip(self, value):
        raw, compressed = self.s.serialize(value, DTYPE_BYTES)
        return self.s.deserialize(raw, compressed, DTYPE_BYTES)

    def test_simple_bytes(self):
        data = b'\x00\x01\x02\xff'
        self.assertEqual(self._roundtrip(data), data)

    def test_bytearray_accepted(self):
        data = bytearray(b'hello')
        result = self._roundtrip(data)
        self.assertEqual(result, bytes(data))

    def test_returns_bytes_type(self):
        result = self._roundtrip(b'test')
        self.assertIsInstance(result, bytes)

    def test_empty_bytes(self):
        self.assertEqual(self._roundtrip(b''), b'')

    def test_wrong_type_raises(self):
        with self.assertRaises(TypeError):
            self.s.serialize('not bytes', DTYPE_BYTES)

    def test_int_raises(self):
        with self.assertRaises(TypeError):
            self.s.serialize(42, DTYPE_BYTES)


class TestSerializerCompression(unittest.TestCase):
    """Compression behaviour across dtypes."""

    THRESHOLD = 100

    def setUp(self):
        self.s = Serializer(
            compress=True,
            compress_threshold=self.THRESHOLD,
            compress_level=6,
        )
        self.s_off = Serializer(compress=False)

    def test_json_compressed_when_above_threshold(self):
        large = {'data': 'x' * 200}
        raw, is_compressed = self.s.serialize(large, DTYPE_JSON)
        self.assertTrue(is_compressed)
        # Verify it's valid zlib
        self.assertIsNotNone(zlib.decompress(raw))

    def test_json_not_compressed_below_threshold(self):
        small = {'k': 'v'}
        _, is_compressed = self.s.serialize(small, DTYPE_JSON)
        self.assertFalse(is_compressed)

    def test_str_compressed_when_above_threshold(self):
        large_str = 'a' * (self.THRESHOLD + 1)
        _, is_compressed = self.s.serialize(large_str, DTYPE_STR)
        self.assertTrue(is_compressed)

    def test_bytes_compressed_when_above_threshold(self):
        large_bytes = bytes(self.THRESHOLD + 1)
        _, is_compressed = self.s.serialize(large_bytes, DTYPE_BYTES)
        self.assertTrue(is_compressed)

    def test_compression_disabled_ignores_size(self):
        large = {'data': 'x' * 500}
        _, is_compressed = self.s_off.serialize(large, DTYPE_JSON)
        self.assertFalse(is_compressed)

    def test_roundtrip_with_compression_json(self):
        large = {'items': list(range(100))}
        raw, is_compressed = self.s.serialize(large, DTYPE_JSON)
        result = self.s.deserialize(raw, is_compressed, DTYPE_JSON)
        self.assertEqual(result, large)

    def test_roundtrip_with_compression_str(self):
        large_str = 'hello ' * 50
        raw, is_compressed = self.s.serialize(large_str, DTYPE_STR)
        result = self.s.deserialize(raw, is_compressed, DTYPE_STR)
        self.assertEqual(result, large_str)

    def test_roundtrip_with_compression_bytes(self):
        large_bytes = b'\xde\xad\xbe\xef' * 50
        raw, is_compressed = self.s.serialize(large_bytes, DTYPE_BYTES)
        result = self.s.deserialize(raw, is_compressed, DTYPE_BYTES)
        self.assertEqual(result, large_bytes)


class TestSerializerInvalidDtype(unittest.TestCase):
    """Invalid dtype handling."""

    def setUp(self):
        self.s = Serializer()

    def test_invalid_dtype_serialize_raises(self):
        with self.assertRaises(TypeError) as ctx:
            self.s.serialize('value', 'xml')
        self.assertIn('xml', str(ctx.exception))

    def test_invalid_dtype_deserialize_raises(self):
        # Manually craft bytes; deserialize with invalid dtype
        with self.assertRaises((TypeError, KeyError, ValueError)):
            self.s.deserialize(b'{}', False, 'xml')


if __name__ == '__main__':
    unittest.main()
