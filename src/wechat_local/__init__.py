"""Local, offline processing for user-authorized WeChat snapshots."""

from .codex_first import ScanResult, TimeWindow

__version__ = "0.2.0"

__all__ = ("CodexFirstRuntime", "ScanResult", "TimeWindow")


def __getattr__(name: str):
    if name == "CodexFirstRuntime":
        from .codex_first_runtime import CodexFirstRuntime

        return CodexFirstRuntime
    raise AttributeError(name)
