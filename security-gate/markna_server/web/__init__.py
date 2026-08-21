"""Server-rendered web UI."""

from .views import PUBLIC_PATHS, render_error, router

__all__ = ["PUBLIC_PATHS", "render_error", "router"]
