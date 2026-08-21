"""MARKNA Security Gate — server-hosted web application.

Layering, outermost first:

* ``api`` / ``web``   — HTTP surfaces. They call ``service`` and never touch the
  scanner engine or a repository directly.
* ``service``         — application services. Enforce access control, then talk
  to storage. The only place both HTTP surfaces share behaviour.
* ``domain``          — entities and invariants. No I/O, no framework.
* ``storage``         — repository protocols plus the SQLite implementation.
* ``identity``        — principals, roles and pluggable authentication.
* ``execution``       — the queue and the worker that actually run scanners, in
  a separate process from anything user-facing.

The scanner engine itself lives in the sibling ``markna`` package and knows
nothing about organisations, projects or HTTP.
"""

__version__ = "1.0.0"
__all__ = ["__version__"]
