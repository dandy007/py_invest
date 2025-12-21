"""Stocks package initialization and runtime patches."""

from __future__ import annotations

import importlib
import sys
import types


def _ensure_websockets_asyncio() -> None:
    """Create a compatibility alias for `websockets.asyncio` if missing."""
    try:
        import websockets
    except ModuleNotFoundError:
        return

    package_name = "websockets.asyncio"
    package = sys.modules.get(package_name)
    if package is None:
        package = types.ModuleType(package_name)
        package.__path__ = []  # type: ignore[attr-defined]
        sys.modules[package_name] = package

    if not hasattr(websockets, "asyncio"):
        setattr(websockets, "asyncio", package)

    mappings = {
        "client": "client",
        "connection": "connection",
        "server": "server",
        "framing": "legacy.framing",
        "protocol": "legacy.protocol",
    }

    for alias, target in mappings.items():
        fullname = f"{package_name}.{alias}"
        if fullname in sys.modules:
            continue
        try:
            target_module = importlib.import_module(f"websockets.{target}")
        except ModuleNotFoundError:
            continue

        sys.modules[fullname] = target_module
        setattr(package, alias, target_module)


_ensure_websockets_asyncio()
