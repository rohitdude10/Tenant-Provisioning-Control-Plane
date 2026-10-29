import threading
import time
from collections.abc import Callable
from pathlib import Path

SHUTDOWN_REQUEST = Path(__file__).resolve().parents[2] / ".run" / "shutdown.request"


def watch_shutdown(callback: Callable[[], None], poll_seconds: float = 0.25) -> None:
    def watch() -> None:
        while not SHUTDOWN_REQUEST.exists():
            time.sleep(poll_seconds)
        callback()

    threading.Thread(target=watch, name="local-shutdown-watcher", daemon=True).start()


def shutdown_event(poll_seconds: float = 0.25) -> threading.Event:
    stopped = threading.Event()
    watch_shutdown(stopped.set, poll_seconds)
    return stopped
