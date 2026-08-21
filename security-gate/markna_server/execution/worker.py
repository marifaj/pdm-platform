"""The worker process.

Separate from the web process on purpose. Scanners shell out to third-party
binaries, pull container images and can run for minutes; none of that belongs in
a request handler. The two processes share only the database, and the handoff is
a row whose status moves ``queued`` → ``running`` → terminal.

Claiming is a conditional UPDATE, so running several workers is safe: two
workers racing for the same run produce one winner. Scaling out is starting
another process.
"""

from __future__ import annotations

import logging
import os
import socket
import threading
import time
from typing import Optional

from ..config import ServerConfig
from ..domain import AssessmentRun
from ..storage import UnitOfWork
from .adapter import RunExecutor

LOGGER = logging.getLogger("markna.worker")


def default_worker_id() -> str:
    return f"{socket.gethostname()}:{os.getpid()}"


class Worker:
    """Claim queued runs and execute them until asked to stop."""

    def __init__(
        self,
        uow: UnitOfWork,
        config: ServerConfig,
        *,
        worker_id: Optional[str] = None,
        executor: Optional[RunExecutor] = None,
    ) -> None:
        self.uow = uow
        self.config = config
        self.worker_id = worker_id or default_worker_id()
        self.executor = executor or RunExecutor(uow, config)

    def claim(self) -> Optional[AssessmentRun]:
        return self.uow.runs.claim_next_queued(self.worker_id)

    def run_once(self) -> Optional[AssessmentRun]:
        """Execute at most one queued run. Returns it, or ``None`` if idle."""
        run = self.claim()
        if run is None:
            return None
        LOGGER.info(
            "claimed run %s (organization=%s project=%s layers=%s)",
            run.id,
            run.organization_id,
            run.project_id,
            ",".join(layer.value for layer in run.layers),
        )
        started = time.monotonic()
        completed = self.executor.execute(run)
        LOGGER.info(
            "finished run %s status=%s verdict=%s in %.1fs%s",
            completed.id,
            completed.status.value,
            completed.verdict.value if completed.verdict else "n/a",
            time.monotonic() - started,
            f" error={completed.error}" if completed.error else "",
        )
        return completed

    def run_forever(self, stop: Optional[threading.Event] = None) -> None:
        stop = stop or threading.Event()
        LOGGER.info("worker %s polling every %.1fs", self.worker_id, self.config.worker_poll_seconds)
        while not stop.is_set():
            try:
                executed = self.run_once()
            except Exception:  # noqa: BLE001 - a worker must not die on one bad run
                LOGGER.exception("worker %s failed while executing a run", self.worker_id)
                executed = None
            if executed is None:
                stop.wait(self.config.worker_poll_seconds)
        LOGGER.info("worker %s stopped", self.worker_id)

    def drain(self, limit: int = 100) -> int:
        """Execute queued runs until the queue is empty. Used by tests and CI."""
        executed = 0
        while executed < limit and self.run_once() is not None:
            executed += 1
        return executed
