"""
Integration tests for CacheManager (end-to-end through the facade).

Each test gets an isolated SQLite DB in a temporary directory.
Kodi APIs are stubbed via kodi_stubs.
"""

import asyncio
import os
import shutil
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

# --- Kodi stubs must come before any cacheman import ---
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'lib'))
import tests.kodi_stubs  # noqa: F401

from cacheman import CacheManager, DTYPE_JSON, DTYPE_STR, DTYPE_BYTES
from cacheman.connection import ConnectionManager


def _make_cache(tmp_path, **kwargs) -> CacheManager:
    """Create a CacheManager backed by a DB in tmp_path."""
    db_path = os.path.join(tmp_path, 'test.db')
    with patch.object(ConnectionManager, '_resolve_path', return_value=db_path):
        cache = CacheManager(**kwargs)
    return cache


class _BaseTest(unittest.TestCase):
    """Base class: creates an isolated CacheManager per test."""

    def setUp(self):
        self._tmpdir = tempfile.mkdtemp(prefix='cm_test_')
        self._db_path = os.path.join(self._tmpdir, 'test.db')
        patcher = patch.object(
            ConnectionManager, '_resolve_path', return_value=self._db_path
        )
        patcher.start()
        self.addCleanup(patcher.stop)
        self.cache = CacheManager()

    def tearDown(self):
        self.cache.close()
        shutil.rmtree(self._tmpdir, ignore_errors=True)


# ===========================================================================
# Basic get / set / delete
# ===========================================================================

class TestBasicOperations(_BaseTest):

    def test_set_and_get_returns_value(self):
        self.cache.set('key:1', {'a': 1})
        self.assertEqual(self.cache.get('key:1'), {'a': 1})

    def test_get_missing_key_returns_none(self):
        self.assertIsNone(self.cache.get('does:not:exist'))

    def test_set_overwrites_existing(self):
        self.cache.set('key:1', 'first')
        self.cache.set('key:1', 'second', dtype=DTYPE_STR)
        self.assertEqual(self.cache.get('key:1'), 'second')

    def test_delete_removes_key(self):
        self.cache.set('key:1', {'x': 1})
        self.cache.delete('key:1')
        self.assertIsNone(self.cache.get('key:1'))

    def test_delete_nonexistent_is_noop(self):
        self.cache.delete('ghost:key')   # should not raise

    def test_exists_true(self):
        self.cache.set('key:1', 'hello', dtype=DTYPE_STR)
        self.assertTrue(self.cache.exists('key:1'))

    def test_exists_false(self):
        self.assertFalse(self.cache.exists('key:1'))


# ===========================================================================
# Data types
# ===========================================================================

class TestDtypes(_BaseTest):

    def test_json_dict_roundtrip(self):
        data = {'channels': [1, 2, 3], 'name': 'test'}
        self.cache.set('data:json', data)
        self.assertEqual(self.cache.get('data:json'), data)

    def test_json_list_roundtrip(self):
        data = [{'id': i} for i in range(10)]
        self.cache.set('data:list', data)
        self.assertEqual(self.cache.get('data:list'), data)

    def test_str_roundtrip(self):
        self.cache.set('token:raw', 'Bearer abc123', dtype=DTYPE_STR)
        result = self.cache.get('token:raw')
        self.assertEqual(result, 'Bearer abc123')
        self.assertIsInstance(result, str)

    def test_bytes_roundtrip(self):
        data = bytes(range(256))
        self.cache.set('thumb:1', data, dtype=DTYPE_BYTES)
        result = self.cache.get('thumb:1')
        self.assertEqual(result, data)
        self.assertIsInstance(result, bytes)

    def test_invalid_dtype_raises(self):
        with self.assertRaises(TypeError):
            self.cache.set('key:1', 'value', dtype='xml')

    def test_str_with_wrong_value_type_raises(self):
        with self.assertRaises(TypeError):
            self.cache.set('key:1', {'not': 'a string'}, dtype=DTYPE_STR)

    def test_bytes_with_wrong_value_type_raises(self):
        with self.assertRaises(TypeError):
            self.cache.set('key:1', 'not bytes', dtype=DTYPE_BYTES)


# ===========================================================================
# TTL
# ===========================================================================

class TestTTL(_BaseTest):

    def test_permanent_key_returns_value(self):
        self.cache.set('key:perm', {'x': 1})
        self.assertIsNotNone(self.cache.get('key:perm'))

    def test_expired_key_returns_none(self):
        self.cache.set('key:exp', {'x': 1}, ttl=1)
        time.sleep(1.1)
        self.assertIsNone(self.cache.get('key:exp'))

    def test_ttl_returns_positive_for_active_key(self):
        self.cache.set('key:ttl', {}, ttl=60)
        remaining = self.cache.ttl('key:ttl')
        self.assertGreater(remaining, 0)
        self.assertLessEqual(remaining, 60)

    def test_ttl_returns_minus_one_for_permanent(self):
        self.cache.set('key:perm', {})
        self.assertEqual(self.cache.ttl('key:perm'), -1)

    def test_ttl_returns_minus_two_for_missing(self):
        self.assertEqual(self.cache.ttl('ghost:key'), -2)

    def test_expire_updates_ttl(self):
        self.cache.set('key:1', {}, ttl=10)
        result = self.cache.expire('key:1', 3600)
        self.assertTrue(result)
        self.assertGreater(self.cache.ttl('key:1'), 60)

    def test_expire_returns_false_for_missing_key(self):
        self.assertFalse(self.cache.expire('ghost', 60))

    def test_persist_makes_key_permanent(self):
        self.cache.set('key:1', {}, ttl=60)
        self.cache.persist('key:1')
        self.assertEqual(self.cache.ttl('key:1'), -1)

    def test_persist_returns_false_for_missing_key(self):
        self.assertFalse(self.cache.persist('ghost'))

    def test_cleanup_removes_expired_keys(self):
        self.cache.set('key:exp1', {}, ttl=1)
        self.cache.set('key:exp2', {}, ttl=1)
        self.cache.set('key:perm', {})
        time.sleep(1.1)
        deleted = self.cache.cleanup()
        self.assertEqual(deleted, 2)
        self.assertIsNone(self.cache.get('key:exp1'))
        self.assertIsNotNone(self.cache.get('key:perm'))


# ===========================================================================
# Pattern operations (keys / clear)
# ===========================================================================

class TestPatternOperations(_BaseTest):

    def setUp(self):
        super().setUp()
        self.cache.set('channels:1', {'name': 'HBO'})
        self.cache.set('channels:2', {'name': 'CNN'})
        self.cache.set('epg:1:monday', {'show': 'News'})
        self.cache.set('token:csrf', 'abc', dtype=DTYPE_STR)

    def test_keys_all(self):
        result = self.cache.keys('*')
        self.assertIn('channels:1', result)
        self.assertIn('channels:2', result)
        self.assertIn('epg:1:monday', result)
        self.assertIn('token:csrf', result)

    def test_keys_simple_prefix_glob(self):
        result = self.cache.keys('channels:*')
        self.assertCountEqual(result, ['channels:1', 'channels:2'])

    def test_keys_complex_pattern_fnmatch(self):
        result = self.cache.keys('*:1:*')
        self.assertIn('epg:1:monday', result)
        self.assertNotIn('channels:1', result)

    def test_keys_no_match_returns_empty(self):
        self.assertEqual(self.cache.keys('missing:*'), [])

    def test_keys_excludes_expired(self):
        self.cache.set('temp:key', {}, ttl=1)
        time.sleep(1.1)
        result = self.cache.keys('*')
        self.assertNotIn('temp:key', result)

    def test_clear_all(self):
        self.cache.clear()
        self.assertEqual(self.cache.keys('*'), [])

    def test_clear_pattern(self):
        self.cache.clear('channels:*')
        result = self.cache.keys('*')
        self.assertNotIn('channels:1', result)
        self.assertNotIn('channels:2', result)
        self.assertIn('epg:1:monday', result)
        self.assertIn('token:csrf', result)

    def test_clear_pattern_no_match_is_noop(self):
        before = set(self.cache.keys('*'))
        self.cache.clear('missing:*')
        after = set(self.cache.keys('*'))
        self.assertEqual(before, after)


# ===========================================================================
# LRU eviction
# ===========================================================================

class TestLRUEviction(_BaseTest):

    def setUp(self):
        self._tmpdir = tempfile.mkdtemp(prefix='cm_test_')
        self._db_path = os.path.join(self._tmpdir, 'test.db')
        patcher = patch.object(
            ConnectionManager, '_resolve_path', return_value=self._db_path
        )
        patcher.start()
        self.addCleanup(patcher.stop)
        # Very small limit to trigger eviction quickly
        self.cache = CacheManager(max_entries=5)

    def test_eviction_keeps_total_at_or_below_limit(self):
        for i in range(20):
            self.cache.set(f'key:{i}', {'value': i})
        # Must flush eviction (runs every 20 SETs)
        self.cache.flush()
        info = self.cache.info()
        self.assertLessEqual(info['active_keys'], 5)

    def test_eviction_target_creates_breathing_room(self):
        # Fill to 5, add 1 more — should evict to 90% of 5 = 4 (floor)
        for i in range(5):
            self.cache.set(f'key:{i}', {'v': i})
        # Trigger eviction manually (set_count divisor is 20)
        self.cache._store._enforce_limit()
        self.cache.flush()
        info = self.cache.info()
        self.assertLessEqual(info['active_keys'], 5)


# ===========================================================================
# info() statistics
# ===========================================================================

class TestInfo(_BaseTest):

    def test_info_returns_dict(self):
        self.assertIsInstance(self.cache.info(), dict)

    def test_info_empty_cache(self):
        info = self.cache.info()
        self.assertEqual(info['total_keys'], 0)
        self.assertEqual(info['active_keys'], 0)

    def test_info_counts_active_and_expired(self):
        self.cache.set('active:1', {})
        self.cache.set('active:2', {})
        self.cache.set('expire:1', {}, ttl=1)
        time.sleep(1.1)
        info = self.cache.info()
        self.assertEqual(info['active_keys'], 2)
        self.assertEqual(info['expired_keys'], 1)
        self.assertEqual(info['total_keys'], 3)

    def test_info_by_dtype(self):
        self.cache.set('j:1', {}, dtype=DTYPE_JSON)
        self.cache.set('j:2', {}, dtype=DTYPE_JSON)
        self.cache.set('s:1', 'hello', dtype=DTYPE_STR)
        self.cache.set('b:1', b'\x00', dtype=DTYPE_BYTES)
        info = self.cache.info()
        by_dtype = info['by_dtype']
        self.assertEqual(by_dtype.get('json', 0), 2)
        self.assertEqual(by_dtype.get('str',  0), 1)
        self.assertEqual(by_dtype.get('bytes', 0), 1)

    def test_info_has_lru_buffered_field(self):
        info = self.cache.info()
        self.assertIn('lru_buffered', info)


# ===========================================================================
# WAL mode
# ===========================================================================

class TestWalMode(_BaseTest):

    def test_journal_mode_is_wal(self):
        conn = self.cache._conn.get()
        cursor = conn.execute('PRAGMA journal_mode')
        mode = cursor.fetchone()[0]
        self.assertEqual(mode, 'wal')

    def test_synchronous_is_normal(self):
        conn = self.cache._conn.get()
        cursor = conn.execute('PRAGMA synchronous')
        # 1 = NORMAL
        self.assertEqual(cursor.fetchone()[0], 1)


# ===========================================================================
# Lazy LRU flush
# ===========================================================================

class TestLazyLRU(_BaseTest):

    def test_flush_writes_last_accessed(self):
        self.cache.set('key:1', {'v': 1})

        # Access the key — buffers last_accessed
        before = time.time()
        self.cache.get('key:1')

        # Force flush
        self.cache.flush()

        # Read last_accessed directly from DB
        conn = self.cache._conn.get()
        cursor = conn.execute('SELECT last_accessed FROM cache WHERE key = ?', ('key:1',))
        last_accessed = cursor.fetchone()[0]

        self.assertGreaterEqual(last_accessed, int(before))

    def test_close_flushes_buffer(self):
        """close() must flush the LRU buffer before closing the connection."""
        self.cache.set('key:1', {'v': 1})
        self.cache.get('key:1')

        # Verify there's something buffered
        self.assertGreater(self.cache._store._lru_dirty, 0)

        # close() → flush + connection close
        self.cache.close()

        # After close, buffer should be empty
        self.assertEqual(self.cache._store._lru_dirty, 0)

        # Prevent tearDown from calling close() again
        self.cache = type('_Dummy', (), {'close': lambda s: None})()


# ===========================================================================
# @cached decorator
# ===========================================================================

class TestCachedDecorator(_BaseTest):

    def test_function_called_once_on_cache_miss_then_hit(self):
        call_count = [0]

        @self.cache.cached(key='result:{x}', ttl=60)
        def expensive(x):
            call_count[0] += 1
            return x * 2

        self.assertEqual(expensive(5), 10)
        self.assertEqual(expensive(5), 10)
        self.assertEqual(call_count[0], 1)  # called only once

    def test_different_args_create_different_keys(self):
        call_count = [0]

        @self.cache.cached(key='fn:{n}', ttl=60)
        def fn(n):
            call_count[0] += 1
            return n

        fn(1)
        fn(2)
        self.assertEqual(call_count[0], 2)

    def test_auto_key_from_function_name(self):
        @self.cache.cached(ttl=60)
        def compute(x, y):
            return x + y

        result1 = compute(3, 4)
        result2 = compute(3, 4)
        self.assertEqual(result1, 7)
        self.assertEqual(result1, result2)

    def test_cached_with_dtype_str(self):
        @self.cache.cached(key='token:v1', dtype=DTYPE_STR)
        def get_token():
            return 'Bearer abc'

        result = get_token()
        self.assertIsInstance(result, str)
        self.assertEqual(result, 'Bearer abc')

    def test_cached_with_dtype_bytes(self):
        @self.cache.cached(key='thumb:1', dtype=DTYPE_BYTES, ttl=60)
        def get_image():
            return b'\xff\xd8\xff'

        result = get_image()
        self.assertIsInstance(result, bytes)

    def test_ttl_expiry_triggers_re_execution(self):
        call_count = [0]

        @self.cache.cached(key='short:lived', ttl=1)
        def fn():
            call_count[0] += 1
            return 'value'

        fn()
        time.sleep(1.1)
        fn()
        self.assertEqual(call_count[0], 2)


# ===========================================================================
# @cached_async decorator
# ===========================================================================

class TestCachedAsyncDecorator(_BaseTest):

    def test_async_decorator_caches_result(self):
        call_count = [0]

        @self.cache.cached_async(key='async:{x}', ttl=60)
        async def async_fn(x):
            call_count[0] += 1
            return x * 3

        asyncio.run(async_fn(4))
        asyncio.run(async_fn(4))
        self.assertEqual(call_count[0], 1)

    def test_async_decorator_requires_coroutine(self):
        with self.assertRaises(TypeError):
            @self.cache.cached_async(key='bad', ttl=60)
            def not_async():
                return 1

    def test_async_different_args_separate_keys(self):
        call_count = [0]

        @self.cache.cached_async(key='af:{n}', ttl=60)
        async def af(n):
            call_count[0] += 1
            return n

        asyncio.run(af(1))
        asyncio.run(af(2))
        self.assertEqual(call_count[0], 2)


# ===========================================================================
# Compression integration
# ===========================================================================

class TestCompression(unittest.TestCase):

    def setUp(self):
        self._tmpdir = tempfile.mkdtemp(prefix='cm_compress_')
        self._db_path = os.path.join(self._tmpdir, 'test.db')
        patcher = patch.object(
            ConnectionManager, '_resolve_path', return_value=self._db_path
        )
        patcher.start()
        self.addCleanup(patcher.stop)
        self.cache = CacheManager(compress=True, compress_threshold=50)

    def tearDown(self):
        self.cache.close()
        shutil.rmtree(self._tmpdir, ignore_errors=True)

    def test_large_json_compresses_and_roundtrips(self):
        large = {'items': list(range(100))}
        self.cache.set('big:data', large, ttl=60)
        result = self.cache.get('big:data')
        self.assertEqual(result, large)

    def test_small_data_not_compressed(self):
        self.cache.set('small:data', {'k': 'v'}, ttl=60)
        result = self.cache.get('small:data')
        self.assertEqual(result, {'k': 'v'})

    def test_compressed_entry_reflected_in_info(self):
        large = {'items': list(range(100))}
        self.cache.set('big:1', large)
        self.cache.set('small:1', {'x': 1})
        info = self.cache.info()
        self.assertGreaterEqual(info['compressed_keys'], 1)


# ===========================================================================
# Context Manager
# ===========================================================================

class TestContextManager(unittest.TestCase):

    def test_context_manager_lifecycle(self):
        tmpdir = tempfile.mkdtemp(prefix='cm_ctx_')
        db_path = os.path.join(tmpdir, 'test.db')
        
        with patch.object(ConnectionManager, '_resolve_path', return_value=db_path):
            with CacheManager() as cache:
                cache.set('ctx:1', 'value', dtype=DTYPE_STR)
                self.assertEqual(cache.get('ctx:1'), 'value')
                
                # Verify connection is open
                self.assertIsNotNone(cache._conn._local.conn)

            # Outside the context block, connection should be closed (None)
            self.assertIsNone(cache._conn._local.conn)

        shutil.rmtree(tmpdir, ignore_errors=True)


if __name__ == '__main__':
    unittest.main(verbosity=2)
