"""Small process-lifecycle helpers shared by background service entry points."""

import asyncio
import signal


def install_stop_handlers(stop_event: asyncio.Event) -> None:
    """Translate SIGINT/SIGTERM into cooperative async shutdown where supported."""

    loop = asyncio.get_running_loop()
    for process_signal in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(process_signal, stop_event.set)
        except NotImplementedError:
            continue
