"""
Middleware to:

#. JSONP-encode requests which specify a `callback` parameter.
#. Modify Django's builtin caching mechanism to include/cache GET parameters.
"""
from django.core.cache import cache
from django.middleware.cache import UpdateCacheMiddleware, FetchFromCacheMiddleware
from django.utils.cache import get_cache_key, get_max_age, learn_cache_key, patch_response_headers

class JSONPEncodeMiddleware(object):
    def process_request(self, request):
        request.GET = request.GET.copy()
        request._callback = request.GET.pop('callback', [None])[0]

    def process_response(self, request, response):
        if request._callback:
            response.content = '%s(%s)' % (request._callback, response.content)
            response['Content-Type'] = 'application/javascript'
        return response

class CacheRequestWrapper(object):
    def __init__(self, request):
        self.request = request

    def __getattr__(self, key):
        if key == 'path':
            from urllib import urlencode
            return '%s?%s' % (self.request.path,
                              urlencode(sorted(self.request.GET.items())))
        return getattr(self.request, key)

class UpdateCacheMiddleware(UpdateCacheMiddleware):
    def process_response(self, request, response):
        """Sets the cache, if needed."""
        if not hasattr(request, '_cache_update_cache') or not request._cache_update_cache:
            # We don't need to update the cache, just return.
            return response
        if request.method != 'GET':
            # This is a stronger requirement than above. It is needed
            # because of interactions between this middleware and the
            # HTTPMiddleware, which throws the body of a HEAD-request
            # away before this middleware gets a chance to cache it.
            return response
        if not response.status_code == 200:
            return response
        # Try to get the timeout from the "max-age" section of the "Cache-
        # Control" header before reverting to using the default cache_timeout
        # length.
        timeout = get_max_age(response)
        if timeout == None:
            timeout = self.cache_timeout
        elif timeout == 0:
            # max-age was set to 0, don't bother caching.
            return response
        patch_response_headers(response, timeout)
        if timeout:
            cache_key = learn_cache_key(CacheRequestWrapper(request),
                                        response,
                                        timeout,
                                        self.key_prefix)
            cache.set(cache_key, response, timeout)
            return response

class FetchFromCacheMiddleware(FetchFromCacheMiddleware):
    def process_request(self, request):
        """
        Checks whether the page is already cached and returns the cached
        version if available.
        """
        if not request.method in ('GET', 'HEAD'):
            request._cache_update_cache = False
            return None # Don't bother checking the cache.

        cache_key = get_cache_key(CacheRequestWrapper(request), self.key_prefix)
        if cache_key is None:
            request._cache_update_cache = True
            return None # No cache information available, need to rebuild.

        response = cache.get(cache_key, None)
        if response is None:
            request._cache_update_cache = True
            return None # No cache information available, need to rebuild.

        request._cache_update_cache = False
        return response
