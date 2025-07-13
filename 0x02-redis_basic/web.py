#!/usr/bin/env python3
"""
Web caching and tracking module.
Implements a function to fetch and cache web page content with access counting.
"""

import redis
import requests
from typing import Callable
from functools import wraps


def track_and_cache(method: Callable) -> Callable:
    """
    Decorator to track URL access counts and cache the result with a 10-second expiration.

    Args:
        method (Callable): The function to decorate (get_page).

    Returns:
        Callable: The wrapped function that tracks access and caches results.
    """
    @wraps(method)
    def wrapper(url: str) -> str:
        """
        Wrapper function that increments access count, checks cache, and fetches/caches content.

        Args:
            url (str): The URL to fetch.

        Returns:
            str: The HTML content of the URL.
        """
        redis_client = redis.Redis()
        count_key = f"count:{url}"
        cache_key = f"cache:{url}"

        # Increment access count
        redis_client.incr(count_key)

        # Check if content is cached
        cached_content = redis_client.get(cache_key)
        if cached_content:
            return cached_content.decode("utf-8")

        # Fetch content if not cached
        content = method(url)

        # Cache content with 10-second expiration
        redis_client.setex(cache_key, 10, content)
        return content
    return wrapper


@track_and_cache
def get_page(url: str) -> str:
    """
    Fetch the HTML content of a URL.

    Args:
        url (str): The URL to fetch.

    Returns:
        str: The HTML content of the URL.
    """
    response = requests.get(url)
    return response.text