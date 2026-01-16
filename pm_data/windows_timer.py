from __future__ import annotations

from contextlib import contextmanager
import sys
from typing import Iterator


@contextmanager
def windows_high_res_timer(*, enable: bool) -> Iterator[None]:
    if not enable or sys.platform != "win32":
        yield
        return
    try:
        import ctypes

        winmm = ctypes.WinDLL("winmm")
        winmm.timeBeginPeriod(1)
    except Exception:
        yield
        return
    try:
        yield
    finally:
        try:
            winmm.timeEndPeriod(1)
        except Exception:
            pass

