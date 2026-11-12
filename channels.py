"""
PA 10: Channels -- starter (final PA of the semester).

Complete Channel below, plus the producer/consumer driver at the
bottom. See the assignment, Part B, for the full requirements.
Values passed through the channel are GCHeap handles (ints) -- see
gc_heap.py.
"""

import threading
from collections import deque
from typing import Any, Optional

from gc_heap import GCHeap

_CLOSED_SENTINEL = object()

class Channel:
    def __init__(self, capacity: int, heap: GCHeap) -> None:
        if capacity < 1:
            raise ValueError('capacity must be positive')
        self.capacity = capacity
        self.heap = heap
        self.buffer = deque()
        self.condition = threading.Condition()
        self.closed = False

    def put(self, value: Any) -> None:
        with self.condition:
            while len(self.buffer) >= self.capacity and not self.closed:
                self.condition.wait()
            if self.closed:
                raise RuntimeError('channel is closed')
            self.heap.incref(value)
            self.buffer.append(value)
            self.condition.notify_all()

    def get(self):
        with self.condition:
            while not self.buffer and not self.closed:
                self.condition.wait()
            if self.buffer:
                value = self.buffer.popleft()
                self.condition.notify_all()
                return value
            return self.CLOSED

    def close(self) -> None:
        with self.condition:
            self.closed = True
            self.condition.notify_all()

    CLOSED = _CLOSED_SENTINEL

def producer_consumer_driver(heap: GCHeap, values: list, channel: Channel, num_consumers: int = 1):
    """
    Given: a driver you can use as-is (or adapt) for your own testing.
    One producer thread allocates + puts each value in `values` onto
    `channel`, then closes it. `num_consumers` consumer threads pull
    handles off, look up and record their value, then release their
    transferred reference, exiting cleanly on the CLOSED sentinel.
    Returns the list of delivered (handle, value) pairs.
    """
    delivered = []
    delivered_lock = threading.Lock()
    def produce():
        try:
            for v in values:
                handle = heap.allocate(v)
                try:
                    channel.put(handle)
                finally:
                    heap.decref(handle)
        finally:
            channel.close()
    def consume():
        while True:
            handle = channel.get()
            if handle is Channel.CLOSED:
                return
            try:
                value = heap._objects[handle][0]
                with delivered_lock:
                    delivered.append((handle, value))
            finally:
                heap.decref(handle)
    producer_thread = threading.Thread(target=produce)
    consumer_threads = [threading.Thread(target=consume) for _ in range(num_consumers)]
    for t in [producer_thread] + consumer_threads:
        t.start()
    producer_thread.join()
    for t in consumer_threads:
        t.join(timeout=10.0)

    return delivered
