# apps/dashboard/services/slabs/batching.py

def partition_ready(items):
    """
    Split iterable of items into (ready, not_ready).
    """
    ready, not_ready = [], []
    for it in items:
        (ready if it["ready"] else not_ready).append(it)
    return ready, not_ready

def batched(items, size=50):
    """
    Yield lists of length <= size.
    """
    batch = []
    for it in items:
        batch.append(it)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch
