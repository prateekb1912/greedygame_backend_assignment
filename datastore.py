"""This module contains the implementation of the SET command logic."""

import threading
import time
from typing import Tuple


class Datastore:
    def __init__(self):
        self.lock = threading.Condition()
        self.data = {}
        self.queues = {}

    def set(self, key: str, value: str, expiry_time: int = None, condition: str = None) -> Tuple[str, int]:
        """
        Writes the value to the datastore using the key and according to the specified parameters.

        Args:
            key (str): The key under which the given value will be stored.
            value (str): The value to be stored.
            expiry_time (int, optional): Specifies the expiry time of the key in seconds.
                Must contain the prefix EX.
                Defaults to None.
            condition (str, optional): Specifies the decision to take if the key already exists.
                Accepts either NX or XX.
                NX -- Only set the key if it does not already exist.
                XX -- Only set the key if it already exists.
                Defaults to None. The default behavior will be to upsert the value of the key.

        Returns:
            Tuple[str, int]: A tuple containing the response message and status code.

        Raises:
            ValueError: If the given condition is invalid.
        """
        with self.lock:
            if condition == "NX" and key in self.data:
                return 400, "key already exists"
            if condition == "XX" and key not in self.data:
                return 400, "key does not exist"
        
            self.data[key] = value

            if expiry_time:
                timer = threading.Timer(expiry_time, self._delete_key, args=[key])
                timer.start()
            return 200, "Value set successfully"

    def get(self, key):
        """
        Retrieves the value associated with the given key from the data store.

        Args:
            key: The key to retrieve the value for.

        Returns:
            The value associated with the given key, or None if the key does not exist.
        """
        with self.lock:
            if key in self.data:
                return self.data[key]

            return None

    def qpush(self, key, values):
        """
        Appends the specified values to the queue associated with the given key in the data store.

        If the queue does not exist for the given key, a new queue is created.

        Args:
            key: The key of the queue to append the values to.
            *values: The values to append to the queue.
        """
        with self.lock:
            if key not in self.queues:
                self.queues[key] = []

            queue = self.queues[key]
            queue.extend([int(v) for v in values])

    def qpop(self, key):
        """
        Returns the last inserted value from the queue.

        Args:
            key (str): Name of the queue to read from.

        Returns:
            dict: A dictionary containing either the value of the last item in the queue,
                or an error message if the queue is empty or not found.
        """
        with self.lock:

            if key not in self.queues:
                return "queue not found"

            queue = self.queues[key]
            if len(queue) == 0:
                return "queue is empty"

            return queue.pop(0)

    def bqpop(self, key, timeout):
        """
        Blocks and waits for the next value in the queue.

        Args:
            key (str): Name of the queue to read from.

        Returns:
            str: The next value in the queue, or None if the queue is empty.
        """
        if key not in self.queues:
            return {'error':'key not found'}, 404

        queue = self.queues[key]

        with self.lock:
            start_time = time.monotonic()
            end_time = start_time + timeout

            while len(queue) == 0:
                wait_time = end_time - time.monotonic()
                if wait_time <= 0:
                    break

                self.lock.wait(wait_time)

            if len(queue) == 0:
                return {'error': 'timeout reached, no value in queue'}, 400

            value = queue.pop(0)

            return {'value': value}, 200

    def _delete_key(self, key):
        with self.lock:
            if key in self.data:
                del self.data[key]
