#!/usr/bin/env python3
"""
Redis basic operations module.
This module implements a Cache class with Redis-based storage, call counting,
and history tracking functionality.
"""

import redis
import uuid
from typing import Union, Callable, Optional
from functools import wraps


def count_calls(method: Callable) -> Callable:
    """
    Decorator to count the number of times a method is called.
    Stores the count in Redis using the method's qualified name as the key.

    Args:
        method (Callable): The method to decorate.

    Returns:
        Callable: The wrapped function that increments the count and calls the original method.
    """
    @wraps(method)
    def wrapper(self, *args, **kwargs):
        """
        Wrapper function that increments the call count in Redis and calls the original method.

        Args:
            self: The instance of the class.
            *args: Positional arguments passed to the method.
            **kwargs: Keyword arguments passed to the method.

        Returns:
            The result of the original method.
        """
        key = method.__qualname__
        self._redis.incr(key)
        return method(self, *args, **kwargs)
    return wrapper


def call_history(method: Callable) -> Callable:
    """
    Decorator to store the history of inputs and outputs for a method.
    Inputs and outputs are stored in Redis lists with keys derived from the method's qualified name.

    Args:
        method (Callable): The method to decorate.

    Returns:
        Callable: The wrapped function that stores inputs/outputs and calls the original method.
    """
    @wraps(method)
    def wrapper(self, *args, **kwargs):
        """
        Wrapper function that stores input arguments and output in Redis lists and calls the original method.

        Args:
            self: The instance of the class.
            *args: Positional arguments passed to the method.
            **kwargs: Keyword arguments passed to the method.

        Returns:
            The result of the original method.
        """
        input_key = f"{method.__qualname__}:inputs"
        output_key = f"{method.__qualname__}:outputs"
        self._redis.rpush(input_key, str(args))
        result = method(self, *args, **kwargs)
        self._redis.rpush(output_key, str(result))
        return result
    return wrapper


def replay(method: Callable) -> None:
    """
    Displays the history of calls for a particular function.
    Retrieves and prints the inputs and outputs stored in Redis.

    Args:
        method (Callable): The method whose call history is to be displayed.
    """
    redis_instance = redis.Redis()
    qualname = method.__qualname__
    input_key = f"{qualname}:inputs"
    output_key = f"{qualname}:outputs"
    inputs = redis_instance.lrange(input_key, 0, -1)
    outputs = redis_instance.lrange(output_key, 0, -1)
    print(f"{qualname} was called {len(inputs)} times:")
    for inp, out in zip(inputs, outputs):
        print(f"{qualname}(*{inp.decode('utf-8')}) -> {out.decode('utf-8')}")


class Cache:
    """
    A class to handle Redis-based caching operations.
    Provides methods to store and retrieve data with type conversion and call tracking.
    """
    def __init__(self):
        """
        Initialize the Cache with a Redis client and flush the database.
        """
        self._redis = redis.Redis()
        self._redis.flushdb()

    @count_calls
    @call_history
    def store(self, data: Union[str, bytes, int, float]) -> str:
        """
        Store data in Redis with a random key and return the key.

        Args:
            data (Union[str, bytes, int, float]): The data to store.

        Returns:
            str: The random key under which the data is stored.
        """
        key = str(uuid.uuid4())
        self._redis.set(key, data)
        return key

    def get(self, key: str, fn: Optional[Callable] = None) -> Union[str, bytes, int, float, None]:
        """
        Retrieve data from Redis by key, optionally applying a conversion function.

        Args:
            key (str): The key to retrieve data for.
            fn (Optional[Callable]): Optional function to convert the retrieved data.

        Returns:
            Union[str, bytes, int, float, None]: The retrieved data, converted if fn is provided, or None if key doesn't exist.
        """
        data = self._redis.get(key)
        if data is None:
            return None
        return fn(data) if fn else data

    def get_str(self, key: str) -> Optional[str]:
        """
        Retrieve data from Redis by key and convert it to a UTF-8 string.

        Args:
            key (str): The key to retrieve data for.

        Returns:
            Optional[str]: The retrieved data as a string, or None if key doesn't exist.
        """
        return self.get(key, lambda d: d.decode("utf-8"))

    def get_int(self, key: str) -> Optional[int]:
        """
        Retrieve data from Redis by key and convert it to an integer.

        Args:
            key (str): The key to retrieve data for.

        Returns:
            Optional[int]: The retrieved data as an integer, or None if key doesn't exist.
        """
        return self.get(key, int)