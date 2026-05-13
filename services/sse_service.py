import asyncio
import time
from typing import Set

_clients: Set[asyncio.Queue] = set()

# Latest event for polling fallback
_last_event: dict = {"ts": 0, "data": None}


def set_last_event(event_type: str, data: dict):
    _last_event["ts"] = int(time.time() * 1000)
    _last_event["data"] = {"type": event_type, **data}


def get_last_event() -> dict:
    return dict(_last_event)


def subscribe() -> asyncio.Queue:
    q: asyncio.Queue = asyncio.Queue()
    _clients.add(q)
    return q


def unsubscribe(q: asyncio.Queue):
    _clients.discard(q)


async def publish(event_type: str, data: dict):
    set_last_event(event_type, data)
    if not _clients:
        return
    msg = {"type": event_type, **data}
    for q in list(_clients):
        await q.put(msg)
