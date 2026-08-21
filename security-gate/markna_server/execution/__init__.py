"""Scanner execution, deliberately outside the request path.

Importing this package pulls in the scanner engine. The API and web layers must
not import it — ``tests/test_layering.py`` enforces that.
"""

from .adapter import RunExecutor
from .worker import Worker, default_worker_id

__all__ = ["RunExecutor", "Worker", "default_worker_id"]
