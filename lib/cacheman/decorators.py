import inspect
import functools

from .store     import CacheStore
from .constants import DTYPE_JSON

class CacheDecorators:
    """
    Provides @cached and @cached_async decorators backed by a CacheStore.

    Responsibility: generate cache keys from function signatures and
    delegate read/write to the store. It knows nothing about the database,
    serialization, or connection management.

    Key resolution rules:
        1. If `key` is provided → format it as a template using bound args.
           e.g. key="channels:{category_id}" with category_id=5 → "channels:5"
        2. If `key` is None → auto-generate from function name + sorted args.
           e.g. fetch_data(x=1, y=2) → "fetch_data:x=1:y=2"
    """

    def __init__(self, store: CacheStore):
        self._store = store

                                                                            
                
                                                                            

    def cached(self, key: str = None, ttl: int = None, dtype: str = DTYPE_JSON):
        """
        Decorator for caching synchronous functions.

        Args:
            key:   Key template with {param} placeholders. None = auto-key.
            ttl:   Time-to-live in seconds. None = no expiry.
            dtype: Data type: 'json' (default), 'str', 'bytes'.

        Example:
            @cache.cached(key="channels:{category_id}", ttl=3600)
            def fetch_channels(category_id):
                return api_call(category_id)
        """
        def decorator(func):
            sig = inspect.signature(func)

            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                cache_key = self._resolve_key(key, func, sig, args, kwargs)

                result = self._store.get(cache_key)
                if result is not None:
                    return result

                result = func(*args, **kwargs)
                self._store.set(cache_key, result, ttl=ttl, dtype=dtype)
                return result

            return wrapper
        return decorator

    def cached_async(self, key: str = None, ttl: int = None, dtype: str = DTYPE_JSON):
        """
        Decorator for caching asynchronous functions.

        Args:
            key:   Key template with {param} placeholders. None = auto-key.
            ttl:   Time-to-live in seconds. None = no expiry.
            dtype: Data type: 'json' (default), 'str', 'bytes'.

        Example:
            @cache.cached_async(key="epg:{channel_id}", ttl=1800)
            async def fetch_epg(channel_id):
                return await async_api_call(channel_id)
        """
        def decorator(func):
            if not inspect.iscoroutinefunction(func):
                raise TypeError(
                    f"{func.__name__} is not async. "
                    "Use @cached() for synchronous functions."
                )

            sig = inspect.signature(func)

            @functools.wraps(func)
            async def wrapper(*args, **kwargs):
                cache_key = self._resolve_key(key, func, sig, args, kwargs)

                result = self._store.get(cache_key)
                if result is not None:
                    return result

                result = await func(*args, **kwargs)
                self._store.set(cache_key, result, ttl=ttl, dtype=dtype)
                return result

            return wrapper
        return decorator

                                                                            
                  
                                                                            

    @staticmethod
    def _resolve_key(key_template, func, sig, args, kwargs) -> str:
        """
        Build the cache key from the template or auto-generate it.

        Args:
            key_template: User-provided template string or None.
            func:         The decorated function.
            sig:          inspect.Signature of func.
            args:         Positional arguments passed to the function.
            kwargs:       Keyword arguments passed to the function.

        Returns:
            str: The resolved cache key.
        """
        bound = sig.bind(*args, **kwargs)
        bound.apply_defaults()
        params = dict(bound.arguments)

        if key_template:
            return key_template.format(**params)

        parts = ':'.join(f"{k}={v}" for k, v in sorted(params.items()))
        return f"{func.__name__}:{parts}" if parts else func.__name__
